#include <queue>
#include <mutex>
#include <optional>


using namespace std;

class non_empty_queue : public std::exception {
    std::string what_;
public:
    explicit non_empty_queue(std::string msg) { what_ = std::move(msg); }
    [[nodiscard]] const char* what() const noexcept override  { return what_.c_str(); }
};

template<typename T> class QueueThreadSafe{
    queue<T> queue_;
    mutable std::mutex mutex_;
    // Moved out of public interface to prevent races between this
    // and pop().
    bool empty() const {
        return queue_.empty();
    }
public:
    QueueThreadSafe() = default;
    QueueThreadSafe(const QueueThreadSafe<T> &) = delete ;
    QueueThreadSafe& operator=(const QueueThreadSafe<T> &) = delete ;

    QueueThreadSafe(QueueThreadSafe<T>&& other) noexcept(false) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!empty()) {
            throw non_empty_queue("Moving into a non-empty queue"s);
        }
        queue_ = std::move(other.queue_);
    }

    virtual ~QueueThreadSafe() noexcept(false) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!empty()) {
            throw non_empty_queue("Destroying a non-empty queue"s);
        }
    }

    unsigned long size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
    std::optional<T> pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return {};
        }
        T tmp = queue_.front();
        queue_.pop();
        return tmp;
    }
    void push(const T &item) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(item);
    }

};

