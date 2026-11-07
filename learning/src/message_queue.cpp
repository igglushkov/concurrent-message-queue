#include <iostream>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

struct Message {};

class MessageQueue {
public:
    void push(Message message) {
        queue_.push_back(std::move(message));
    }

    size_t size() const {
        return queue_.size();
    }

private:
    std::vector<Message> queue_;
};

void PushMessages(MessageQueue& mq, size_t count) {
    for (size_t i = 0; i < count; i++) {
        mq.push(Message{});
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

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}