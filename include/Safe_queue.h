#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>

template<typename T>
class SafeQueue {
private:
    std::queue<T> queue;
    std::mutex mtx;
    std::condition_variable cv;
public:
    void enqueue(T value) {
        std::unique_lock<std::mutex> lock(mtx);
        queue.push(value);
        cv.notify_one();
    }

    T dequeue() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this] { return !queue.empty(); });
        T value = queue.front();
        queue.pop();
        return value;
    }

    bool empty() {
        std::unique_lock<std::mutex> lock(mtx);
        return queue.empty();
    }
};
