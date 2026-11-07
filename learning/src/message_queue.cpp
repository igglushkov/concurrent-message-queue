#include <iostream>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>
#include <mutex>

#include <gtest/gtest.h>

struct Message {
    size_t id{};
};

class MessageQueue {
public:
    void push(Message message) {
        std::lock_guard<std::mutex> lock(mx_);
        queue_.push_back(std::move(message));
    }

    std::optional<Message> pop() {
        std::lock_guard lock(mx_);

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

private:
    mutable std::mutex mx_;
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