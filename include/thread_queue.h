#include <queue>
#include <mutex>
#include <condition_variable>

template <typename T>
class ThreadSafeQueue {
public:
    void push(T value) {
        std::lock_guard<std::mutex> lock(mtx);
        q.push(std::move(value));
        cv.notify_one();
    }

    bool try_pop(T& value) {
        std::lock_guard<std::mutex> lock(mtx);
        if (q.empty()) {
            return false;
        }
        value = std::move(q.front());
        q.pop();
        return true;
    }

    void wait_and_pop(T& value) {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this]() { return !q.empty(); });
        value = std::move(q.front());
        q.pop();
    }

private:
    std::queue<T> q;
    std::mutex mtx;
    std::condition_variable cv;
};
