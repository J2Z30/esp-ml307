#include "esp_tcp.h"

#include <esp_log.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>

static const char *TAG = "EspTcp";

EspTcp::EspTcp() {
    event_group_ = xEventGroupCreate();
}

EspTcp::~EspTcp() {
    Disconnect();

    if (event_group_ != nullptr) {
        vEventGroupDelete(event_group_);
        event_group_ = nullptr;
    }
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
    // host is domain
    struct hostent *server = gethostbyname(host.c_str());
    if (server == NULL) {
        ESP_LOGE(TAG, "Failed to get host by name");
        return false;
    }
    memcpy(&server_addr.sin_addr, server->h_addr, server->h_length);

    tcp_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_fd_ < 0) {
        ESP_LOGE(TAG, "Failed to create socket");
        return false;
    }

    struct timeval tv = {
        .tv_sec = 3,
        .tv_usec = 0,
    };

    if (setsockopt(tcp_fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0) {
        ESP_LOGE(TAG, "Fail to setsockopt SO_SNDTIMEO %d", errno);
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }

    if (setsockopt(tcp_fd_, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv)) < 0) {
        ESP_LOGE(TAG, "Fail to setsockopt SO_RCVTIMEO %d", errno);
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }

    int keep_alive_enable = 1;
    int keep_alive_idle = 5;
    int keep_alive_interval = 5;
    int keep_alive_count = 3;
    ESP_LOGD(TAG, "Enable TCP keep alive. idle: %d, interval: %d, count: %d", keep_alive_idle, keep_alive_interval, keep_alive_count);

    if (setsockopt(tcp_fd_, SOL_SOCKET, SO_KEEPALIVE, &keep_alive_enable, sizeof(keep_alive_enable)) < 0) {
        ESP_LOGE(TAG, "Fail to setsockopt SO_KEEPALIVE (%d)", errno);
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }
#ifndef __APPLE__
    if (setsockopt(tcp_fd_, IPPROTO_TCP, TCP_KEEPIDLE, &keep_alive_idle, sizeof(keep_alive_idle)) < 0) {
        ESP_LOGE(TAG, "Fail to setsockoptt TCP_KEEPIDLE (%d)", errno);
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }
    if (setsockopt(tcp_fd_, IPPROTO_TCP, TCP_KEEPINTVL, &keep_alive_interval, sizeof(keep_alive_interval)) < 0) {
        ESP_LOGE(TAG, "Fail to setsockopt TCP_KEEPINTVL (%d)", errno);
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }
    if (setsockopt(tcp_fd_, IPPROTO_TCP, TCP_KEEPCNT, &keep_alive_count, sizeof(keep_alive_count)) < 0) {
        ESP_LOGE(TAG, "Fail to setsockopt TCP_KEEPCNT (%d)", errno);
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }
#else // __APPLE__
    if (setsockopt(tcp_fd_, IPPROTO_TCP, TCP_KEEPALIVE, &keep_alive_idle, sizeof(keep_alive_idle)) < 0) {
        ESP_LOGE(TAG, "Fail to setsockopt TCP_KEEPALIVE (%d)", errno);
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }
#endif // __APPLE__

    int ret = connect(tcp_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr));
    if (ret < 0) {
        ESP_LOGE(TAG, "Failed to connect to %s:%d", host.c_str(), port);
        close(tcp_fd_);
        tcp_fd_ = -1;
        return false;
    }

    connected_ = true;

    xEventGroupClearBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT);
    xTaskCreate([](void* arg) {
        EspTcp* tcp = (EspTcp*)arg;
        tcp->ReceiveTask();
        xEventGroupSetBits(tcp->event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT);
        vTaskDelete(NULL);
    }, "tcp_receive", 4096, this, 1, &receive_task_handle_);
    return true;
}

void EspTcp::Disconnect() {
    connected_ = false;

    if (tcp_fd_ != -1) {
        close(tcp_fd_);
        tcp_fd_ = -1;

        auto bits = xEventGroupWaitBits(event_group_, ESP_TCP_EVENT_RECEIVE_TASK_EXIT, pdFALSE, pdFALSE, pdMS_TO_TICKS(10000));
        if (!(bits & ESP_TCP_EVENT_RECEIVE_TASK_EXIT)) {
            ESP_LOGE(TAG, "Failed to wait for receive task exit");
        }
    }
}

int EspTcp::Send(const std::string& data) {
    if (!connected_ || tcp_fd_ == -1) {
        ESP_LOGE(TAG, "Not connected");
        return -1;
    }

    size_t total_sent = 0;
    size_t data_size = data.size();
    const char* data_ptr = data.data();

    std::lock_guard<std::mutex> lock(send_mutex_);
    
    while (total_sent < data_size) {
        ssize_t ret = send(tcp_fd_, data_ptr + total_sent, data_size - total_sent, 0);
        if (ret > 0) {
            total_sent += ret;
        } else if (ret == 0) {
            ESP_LOGE(TAG, "Send failed %d, connection maybe closed.", ret);
            connected_ = false;
            // 连接断开时调用断连回调
            if (disconnect_callback_) {
                disconnect_callback_();
            }
            return ret;
        } else if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 忽略 EAGAIN 和 EWOULDBLOCK
                vTaskDelay(pdMS_TO_TICKS(10));
                continue;
            } else {
                ESP_LOGE(TAG, "Send failed: ret=%d, errno=%d, %s", ret, errno, strerror(errno));
                connected_ = false;
                // 连接断开时调用断连回调
                if (disconnect_callback_) {
                    disconnect_callback_();
                }
                return ret;
            }
        }
    }
    
    return total_sent;
}

void EspTcp::ReceiveTask() {
    const size_t kMaxDataSize = 1500;
    std::vector<uint8_t> data(kMaxDataSize);

    while (connected_ && tcp_fd_ != -1) {
        ssize_t ret = recv(tcp_fd_, data.data(), data.size(), 0);
        if (ret > 0) {
            if (stream_callback_) {
                stream_callback_(std::string(reinterpret_cast<char*>(data.data()), ret));
            }
        } else if (ret == 0) {
            ESP_LOGW(TAG, "Connection close by peer.");
            break;
        } else if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 忽略 EAGAIN 和 EWOULDBLOCK
                vTaskDelay(pdMS_TO_TICKS(10));
                continue;
            } else {
                ESP_LOGE(TAG, "TCP receive errno: %s(errno %d)", strerror(errno), errno);
                break;
            }
        }
    }

    connected_ = false;
    // 接收失败或连接断开时调用断连回调
    if (disconnect_callback_) {
        disconnect_callback_();
    }
}
