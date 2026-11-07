#include <iostream>
#include <vector>

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

int main() {
    return 0;
}