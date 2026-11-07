#include <condition_variable>
#include <iostream>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

struct Message {
    size_t id{};
};

class MessageQueue {
public:
    bool push(Message message) {
        {
            std::lock_guard<std::mutex> lock(mx_);
            if (shutdown_) {
                return false;
            }
            queue_.push_back(std::move(message));
        }

        cv_.notify_one();
        return true;
    }

    std::optional<Message> pop() {
        std::unique_lock<std::mutex> lock(mx_);

        cv_.wait(lock, [this]() { return !queue_.empty() || shutdown_; });

        if (queue_.empty()) {
            return std::nullopt;
        }

        Message message = queue_.back();
        queue_.pop_back();
        return message;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mx_);
        return queue_.size();
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mx_);
            shutdown_ = true;
        }

        cv_.notify_all();
    }

private:
    bool shutdown_{false};
    mutable std::mutex mx_;
    std::condition_variable cv_;
    std::vector<Message> queue_;
};

void PushMessages(MessageQueue& mq, size_t count) {
    for (size_t i = 0; i < count; i++) {
        mq.push(Message{.id = i});
    }
}

void PopMessages(MessageQueue& mq, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (mq.pop() == std::nullopt) {
            return;
        };
    }
}

TEST(MessageQueue, Push) {
    MessageQueue mq;
    std::vector<std::thread> producers;
    const size_t n_threads = 4;
    const size_t k_message_count = 10'000;
    for (size_t i = 0; i < n_threads; i++) {
        producers.emplace_back(PushMessages, std::ref(mq), k_message_count);
    }

    for (auto&& p : producers) {
        p.join();
    }

    ASSERT_EQ(mq.size(), n_threads * k_message_count);
}

TEST(MessageQueue, Pop) {
    MessageQueue mq;
    std::vector<std::thread> threads;
    const size_t n_threads = 4;
    const size_t k_message_count = 5000;
    for (size_t i = 0; i < n_threads; i++) {
        threads.emplace_back(PushMessages, std::ref(mq), k_message_count);
        threads.emplace_back(PopMessages, std::ref(mq), k_message_count);
    }

    for (auto&& p : threads) {
        p.join();
    }

    ASSERT_EQ(mq.size(), 0);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}