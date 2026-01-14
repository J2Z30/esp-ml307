#include "esp_tcp.h"

#include <esp_log.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>
#include <fcntl.h>

static const char *TAG = "EspTcp";

EspTcp::EspTcp() {
    event_group_ = xEventGroupCreate();
    if (event_group_ == nullptr) {
        ESP_LOGE(TAG, "Failed to create event group");
    }
}

EspTcp::~EspTcp() {
    Disconnect();

    if (event_group_ != nullptr) {
        vEventGroupDelete(event_group_);
        event_group_ = nullptr;
    }

    ESP_LOGI(TAG, "EspTcp destroyed");
}

bool EspTcp::Connect(const std::string& host, int port) {
    // 确保先断开已有连接
    if (connected_) {
        Disconnect();
    }

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    // 注意: gethostbyname 本身是阻塞的。如果需要完全非阻塞 DNS，需要使用其他机制。
    struct hostent *server = gethostbyname(host.c_str());
    if (server == NULL) {
        last_error_ = h_errno;
        ESP_LOGE(TAG, "Failed to get host by name");
        return false;
    }
    memcpy(&server_addr.sin_addr, server->h_addr, server->h_length);

    tcp_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_fd_ < 0) {
        last_error_ = errno;
        ESP_LOGE(TAG, "Failed to create socket");
        return false;
    }

    int flags = fcntl(tcp_fd_, F_GETFL, 0);
    if (flags == -1)
        return false;

    flags = fcntl(tcp_fd_, F_SETFL, flags | O_NONBLOCK);
    if (flags < 0) {
        ESP_LOGE(TAG, "Failed to set non-blocking");
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }

    int ret = connect(tcp_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr));
    if (ret < 0) {
        if (errno == EINPROGRESS) {
            // 连接正在进行中，使用 select 等待可写
            fd_set wd;
            FD_ZERO(&wd);
            FD_SET(tcp_fd_, &wd);

            struct timeval tv;
            tv.tv_sec = 5; // 5秒连接超时
            tv.tv_usec = 0;

            ret = select(tcp_fd_ + 1, NULL, &wd, NULL, &tv);
            if (ret > 0) {
                // select 返回，检查是否有 socket 错误
                int so_error = 0;
                socklen_t len = sizeof(so_error);
                getsockopt(tcp_fd_, SOL_SOCKET, SO_ERROR, &so_error, &len);
                if (so_error == 0) {
                    ESP_LOGI(TAG, "Connect success (async)");
                } else {
                    ESP_LOGE(TAG, "Connect failed after select: %s", strerror(so_error));
                    last_error_ = so_error;
                    close(tcp_fd_);
                    tcp_fd_ = -1;
                    return false;
                }
            } else if (ret == 0) {
                ESP_LOGE(TAG, "Failed to connect timeout %s:%d", host.c_str(), port);
                last_error_ = errno;
                close(tcp_fd_);
                tcp_fd_ = -1;
                return false;
            } else {
                ESP_LOGE(TAG, "Connect select error: %d, %s", errno, strerror(errno));
                last_error_ = errno;
                close(tcp_fd_);
                tcp_fd_ = -1;
                return false;
            }
        } else {
            // 直接失败
            last_error_ = errno;
            ESP_LOGE(TAG, "Failed to connect to %s:%d, %s", host.c_str(), port, strerror(errno));
            close(tcp_fd_);
            tcp_fd_ = -1;
            return false;
        }
    }

    connected_ = true;

    xEventGroupClearBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT);
    xTaskCreate([](void* arg) {
        EspTcp* tcp = (EspTcp*)arg;
        tcp->ReceiveTask();
	    ESP_LOGI(TAG, "ReceiveTask destroyed.");
        vTaskDelete(NULL);
    }, "tcp_receive", 4096, this, 1, &receive_task_handle_);
    return true;
}

void EspTcp::Disconnect() {
    // 如果已经断开，直接返回
    std::unique_lock<std::mutex> lock(mutex_);
    ESP_LOGI(TAG, "Disconnect tcp_fd_ = %d", tcp_fd_);
    if (!connected_) {
        return;
    }
    lock.unlock();

    uint32_t notificationValue = 1;

    if (receive_task_handle_ != nullptr) {
        xTaskNotify(receive_task_handle_, notificationValue, eSetValueWithOverwrite);
    }

    // 主动断开，需要等待接收任务退出
    DoDisconnect(true);
}

void EspTcp::DoDisconnect(bool wait_for_task)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (!connected_) {
        ESP_LOGI(TAG, "Already disconnected");
        xEventGroupSetBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT);
        return;
    }
    connected_ = false;
    lock.unlock();
    ESP_LOGI(TAG, "DoDisconnect tcp_fd_ = %d, wait_for_task = %d", tcp_fd_, wait_for_task);
    if (tcp_fd_ != -1) {
        shutdown(tcp_fd_, SHUT_RDWR);
        close(tcp_fd_);
        tcp_fd_ = -1;

        // 只有主动断开时才需要等待接收任务退出
        // 被动断开时，当前就是接收任务，不需要等待
        if (wait_for_task) {
            auto bits = xEventGroupWaitBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT, pdTRUE, pdFALSE, pdMS_TO_TICKS(10000));
            if (!(bits & ESP_TCP_EVENT_RECEIVE_TASK_EXIT)) {
                ESP_LOGE(TAG, "Failed to wait for receive task exit");
            }
        }

        // 断开连接时触发断开回调
        if (disconnect_callback_) {
            disconnect_callback_();
        }
    }
}

int EspTcp::Send(const std::string& data)
{
    int ret = 0;
    fd_set wd;
    struct timeval tv;

    size_t total_sent = 0;
    size_t data_size = data.size();
    const char* data_ptr = data.data();

    if (!connected_ || tcp_fd_ < 0) {
        ESP_LOGE(TAG, "Not connected");
        return -1;
    }

    while (total_sent < data_size) {
        FD_ZERO(&wd);
        FD_SET(tcp_fd_, &wd);
        tv.tv_sec = 2; // 写超时 2 秒
        tv.tv_usec = 0;

        ret = select(tcp_fd_ + 1, NULL, &wd, NULL, &tv);
        if (ret > 0) {
            ret = send(tcp_fd_, data_ptr + total_sent, data_size - total_sent, 0);
            if (ret < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    continue; // 缓冲区满，重试
                }
                ESP_LOGE(TAG, "Send failed: errno=%d", errno);
                return -1;
            }
            total_sent += ret;
        } else if (ret == 0) {
            ESP_LOGW(TAG, "Send timeout (socket buffer full)");
            break;
        } else {
            ESP_LOGE(TAG, "Send select error: %d", errno);
            DoDisconnect(false); // 发生严重错误断开
            return -1;
        }
    }

    return total_sent;
}

void EspTcp::ReceiveTask()
{
    int ret = 0;
    std::vector<char> buffer(1500);

    fd_set readfds;
    struct timeval tv;

    uint32_t notificationValue;
    BaseType_t notificationReceived;

    while (1) {
        notificationReceived = xTaskNotifyWait(
                0x00,           // 进入时不清除任何位
                ULONG_MAX,      // 退出时清除所有位
                &notificationValue,
                pdMS_TO_TICKS(100)  // 100ms超时
            );
            
        // 检查是否收到停止通知
        if(notificationReceived == pdTRUE) {
            ESP_LOGI(TAG, "ReceiveTask stop");
            xEventGroupSetBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT);
            break;
        }

        FD_ZERO(&readfds);
        FD_SET(tcp_fd_, &readfds);

        tv.tv_sec = 0;
        tv.tv_usec = 500000;  // 100ms


        ret = select(tcp_fd_ + 1, &readfds, NULL, NULL, &tv);
        if (ret > 0) {
            if (FD_ISSET(tcp_fd_, &readfds)) {
                int n = recv(tcp_fd_, buffer.data(), buffer.size(), 0);
                // ESP_LOGI(TAG, "TCP tcp_fd_ %d receive %d bytes", tcp_fd_, n);
                if (n > 0) {
                    if (stream_callback_) {
                        std::string data(buffer.data(), n);
                        stream_callback_(data);
                    }
                } else if (n == 0) {
                    ESP_LOGI(TAG, "TCP receive EOF (Peer closed connection).");
                    // 只有在接收循环里确认是被动关闭，才传 false (不用等任务退出)
                    DoDisconnect(false);
                    break;
                } else {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        // 非阻塞模式下的正常情况（虽然 select 说了可读，但为了保险）
                        continue;
                    }
                    ESP_LOGE(TAG, "TCP recv error: %s (%d)", strerror(errno), errno);
                    DoDisconnect(false);
                    break;
                }
            }
        } else if (ret == 0) {
            // 超时，无数据，循环继续以检查 connected_ 标志
            continue;
        } else {
            // Select 出错 (可能是 socket 被其他线程 close 了)
            if (errno == EBADF || errno == EINTR) {
                ESP_LOGW(TAG, "Select interrupted or bad fd, exiting task");
                DoDisconnect(false);
                break;
            }
            ESP_LOGE(TAG, "Select failed: %d", errno);
            DoDisconnect(false);
            break;
        }
    }
}

int EspTcp::GetLastError() {
    return last_error_;
}
