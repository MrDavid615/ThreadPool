#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <memory>	// 智能指针
#include <thread>
#include <atomic>	// 线程安全原子操作
#include <mutex>	// 互斥锁
#include <condition_variable>
#include <functional>
#include <unordered_map>

// Any 类型
class Any
{
public:
	Any() = default;
	~Any() = default;
	Any(const Any&) = delete;
	Any& operator=(const Any&) = delete;
	Any(Any&&) = default;
	Any& operator=(Any&&) = default;

	template<typename T>
	Any(T data) :base_(std::make_unique<Derive<T>>(data))
	{ }

	template<typename T>
	T cast_()
	{
		// 基类指针转成派生类指针
		Derive<T>* pd = dynamic_cast<Derive<T>*>(base_.get());
		if (pd == NULL)
		{
			throw "type is unmatch!";
		}
		return pd->data_;
	}
private:
	// 基类类型
	class Base
	{
	public:
		virtual ~Base() = default;
	};

	// 派生类
	template<typename T>
	class Derive :public Base
	{
	public:
		Derive(T data):data_(data)
		{}
		T data_;
	};

private:
	// 定义一个基类指针
	std::unique_ptr<Base> base_;
};

// 实现一个信号量
class Semaphore
{
public:
	Semaphore(int resLimit = 0) 
		:resLimit_(resLimit)
	{};
	~Semaphore() = default;

	// 获取一个信号量资源
	void wait()
	{
		std::unique_lock<std::mutex> lock(mtx_);
		// 当信号量资源等于0 阻塞
		cond_.wait(lock, [&]()->bool {return resLimit_ > 0; });
		// 消耗一个信号量
		resLimit_--;
	}

	// 增加一个信号量资源
	void post()
	{
		std::unique_lock<std::mutex> lock(mtx_);
		resLimit_++;
		cond_.notify_all();
	}
private:
	int resLimit_;
	std::mutex mtx_;
	std::condition_variable cond_;
};

class Task;	// 前置声明

// 任务返回值
class Result
{
public:
	Result(std::shared_ptr<Task> task, bool isValid = true);

	~Result() = default;

	void setVal(Any any);

	// 用户调用获取task的值
	Any get();
private:
	Any any_;		// 存储任务的返回值
	Semaphore sem_;	// 线程通信的信号量
	std::shared_ptr<Task> task_;	// 指向对应获取返回值的任务对象
	std::atomic_bool isValid_;	// 是否提交成功
};

// 任务抽象基类
// 用户定义任务类型，从Task继承，重写run方法
class Task
{
public:
	Task();
	~Task() = default;
	void exec();
	void setResult(Result* res);

	virtual Any run() = 0;
private:
	Result* result_;	// 这里不能用强智能指针，交叉引用，永远不释放
};

enum class PoolMode
{
	MODE_FIXED,		// 固定数量线程
	MODE_CACHED,	// 可动态增长线程
};

class Thread
{
public:
	using ThreadFunc = std::function<void(int)>;	// 线程函数对象类型

	Thread(ThreadFunc func);

	~Thread();
	// 启动线程
	void start();

	// 获取线程ID
	int getId() const;
private:
	ThreadFunc func_;
	static int generateId_;
	int threadId_;		// 保存线程ID
	
};

/*
* example:
ThreadPool pool;
pool.start(4);

class MyTask : public Task
{
public:
	void run() {}
};

pool.submitTask(std::make_shared<MyTask>());
*/

class ThreadPool 
{
public:
	ThreadPool();

	~ThreadPool();

	// 设置工作模式
	void setMode(PoolMode mode);

	// 修改阈值
	void setTaskQueMaxThreshHold(int threshhold);

	// 设置Cached模式下线程上限
	void setThreadSizeThreshHold(int threshhold);

	// 提交任务
	Result submitTask(std::shared_ptr<Task> sp);

	//开启线程池
	void start(int initThreadSize = std::thread::hardware_concurrency());

	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;

private:
	// 定义线程函数 用于从任务队列中消费任务
	void threadFunc(int threadid);

	// 检测pool的运行状态
	bool checkRunningState() const;

private:
	// std::vector<std::unique_ptr<Thread>> threads_;	// 线程列表
	std::unordered_map<int, std::unique_ptr<Thread>> threads_;	// 线程列表2.0
	int initThreadSize_;							// 初始线程数量
	std::atomic_int curThreadSize_;					// 记录当前线程总数量
	int threadSizeThreshHold_;						// 线程数量上限阈值
	std::atomic_int idleThreadSize_;				// 记录空闲线程的数量

	// 如果用户提交了一个匿名任务对象，则任务队列可能拿到一个析构的基类指针
	// 设置好生命周期，需要使用智能指针
	std::queue<std::shared_ptr<Task>> taskQue_;	// 任务队列，使用智能指针
	std::atomic_int taskSize_;					// 任务数量，原子操作
	int taskQueMaxThreshHold_;					// 任务队列数量上限阈值

	std::mutex taskQueMtx_;						// 保证任务队列的线程安全
	std::condition_variable notFull_;			// 表示任务队列不满
	std::condition_variable notEmpty_;			// 表示任务队列不空
	std::condition_variable exitCond_;			// 等待线程资源回收

	PoolMode poolMode_;							// 工作模式
	std::atomic_bool isPoolRunning_;			// 表示线程池运行状态
};

#endif // THREADPOOL_H
