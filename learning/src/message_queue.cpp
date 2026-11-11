#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

struct Message {
    size_t id{};
};

template <typename T>
class MessageQueue {
public:
    MessageQueue(size_t max_capacity) : max_capacity_(max_capacity) {}

    template <typename U>
    bool push(U&& message) {
        {
            std::unique_lock<std::mutex> lock(mx_);

            if (!shutdown_ && queue_.size() == max_capacity_) {
                producer_cv_.wait(lock,
                                  [this]() { return queue_.size() < max_capacity_ || shutdown_; });
            }

            if (shutdown_) {
                return false;
            }

            queue_.push_back(std::forward<U>(message));
        }

        consumer_cv_.notify_one();
        return true;
    }

    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mx_);

        consumer_cv_.wait(lock, [this]() { return !queue_.empty() || shutdown_; });

        if (queue_.empty()) {
            return std::nullopt;
        }

        auto message = queue_.front();
        queue_.pop_front();

        producer_cv_.notify_one();

        return message;
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mx_);
            if (shutdown_) {
                return;
            }
            shutdown_ = true;
        }
        consumer_cv_.notify_all();
        producer_cv_.notify_all();
    }

    bool is_enabled() const {
        std::lock_guard<std::mutex> lock(mx_);
        return !shutdown_;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mx_);
        return queue_.size();
    }

    size_t max_capacity() const noexcept {
        return max_capacity_;
    }

private:
    bool shutdown_{false};
    mutable std::mutex mx_;
    std::condition_variable consumer_cv_;
    std::condition_variable producer_cv_;

    size_t max_capacity_{0};
    std::deque<T> queue_;
};

template <typename T>
void PushMessages(MessageQueue<T>& mq, size_t count) {
    for (size_t i = 0; i < count; i++) {
        mq.push(Message{.id = i});
    }
}

template <typename T>
void PopMessages(MessageQueue<T>& mq, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (mq.pop() == std::nullopt) {
            return;
        };
    }
}

template <typename T>
void ShutDown(MessageQueue<T>& mq, size_t count) {
    for (size_t i = 0; i < count; i++) {
        mq.shutdown();
    }
}

TEST(MessageQueue, MaxCapacity) {
    const size_t k_mq_capacity = 5'000;
    MessageQueue<Message> mq(k_mq_capacity);

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    const size_t n_threads = 4;
    const size_t k_message_count = 10000;

    for (size_t i = 0; i < n_threads; i++) {
        producers.emplace_back(PushMessages<Message>, std::ref(mq), k_message_count);
        consumers.emplace_back(PopMessages<Message>, std::ref(mq), k_message_count);
    }

    for (auto&& producer : producers) {
        producer.join();
    }

    for (auto&& consumer : consumers) {
        consumer.join();
    }

    ASSERT_EQ(mq.size(), 0);
}

TEST(MessageQueue, Shutdown) {
    const size_t k_mq_capacity = 50'000;
    MessageQueue<Message> mq(k_mq_capacity);

    std::vector<std::thread> producers;
    const size_t n_threads = 4;
    const size_t k_message_count = 10000;

    for (size_t i = 0; i < n_threads / 2; i++) {
        producers.emplace_back(PushMessages<Message>, std::ref(mq), k_message_count);
    }
    const unsigned k_shutdown_count = 100;
    for (size_t i = 0; i < n_threads / 2; i++) {
        producers.emplace_back(ShutDown<Message>, std::ref(mq), k_shutdown_count);
    }

    for (auto&& producer : producers) {
        producer.join();
    }

    ASSERT_FALSE(mq.is_enabled());
}

TEST(MessageQueue, Push) {
    const size_t k_mq_capacity = 50'000;
    MessageQueue<Message> mq(k_mq_capacity);

    std::vector<std::thread> producers;
    const size_t n_threads = 4;
    const size_t k_message_count = 10'000;
    for (size_t i = 0; i < n_threads; i++) {
        producers.emplace_back(PushMessages<Message>, std::ref(mq), k_message_count);
    }

    for (auto&& p : producers) {
        p.join();
    }

    ASSERT_EQ(mq.size(), n_threads * k_message_count);
}

TEST(MessageQueue, Pop) {
    const size_t k_mq_capacity = 50'000;
    MessageQueue<Message> mq(k_mq_capacity);

    std::vector<std::thread> threads;
    const size_t n_threads = 4;
    const size_t k_message_count = 5000;
    for (size_t i = 0; i < n_threads; i++) {
        threads.emplace_back(PushMessages<Message>, std::ref(mq), k_message_count);
        threads.emplace_back(PopMessages<Message>, std::ref(mq), k_message_count);
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