#include "threadpool.h"
#include <functional>
#include <thread>
#include <iostream>

const int TASK_MAX_THRESHHOLD = 1024;
const int THREAD_MAX_THRESHHOLD = 100;
const int THREAD_MAX_IDLE_TIME = 5;	// 60s

ThreadPool::ThreadPool()
	: initThreadSize_(0)
	, taskSize_(0)
	, taskQueMaxThreshHold_(TASK_MAX_THRESHHOLD)
	, poolMode_(PoolMode::MODE_FIXED)
	, isPoolRunning_(false)
	, idleThreadSize_(0)
	, threadSizeThreshHold_(THREAD_MAX_THRESHHOLD)
	, curThreadSize_(0)
{}

ThreadPool::~ThreadPool()
{
	isPoolRunning_ = false;

	// 等待线程池里所有的线程返回 当前线程有两种状态：阻塞/任务执行中
	std::unique_lock<std::mutex> lock(taskQueMtx_);

	notEmpty_.notify_all();	// 唤醒所有线程

	exitCond_.wait(lock, [&]()->bool {return threads_.size() == 0; });
}

// 设置工作模式
void ThreadPool::setMode(PoolMode mode)
{
	if (checkRunningState()) { return; }
	poolMode_ = mode;
}

// 修改阈值
void ThreadPool::setTaskQueMaxThreshHold(int threshhold)
{
	if (checkRunningState()) { return; }
	taskQueMaxThreshHold_ = threshhold;
}

// 设置Cached模式下线程上限
void ThreadPool::setThreadSizeThreshHold(int threshhold)
{
	if (checkRunningState()) { return; }
	if (poolMode_ == PoolMode::MODE_CACHED) { threadSizeThreshHold_ = threshhold; }
}

// 提交任务
Result ThreadPool::submitTask(std::shared_ptr<Task> sp)
{
	// 获取锁
	std::unique_lock<std::mutex> lock(taskQueMtx_);

	// 线程通信 等待任务队列空余 wait wait_for wait_until
	//while (taskQue_.size() == taskQueMaxThreshHold_)
	//{
	//	notFull_.wait(lock);
	//}
	if (!notFull_.wait_for(lock, std::chrono::seconds(1), 
		[&]()->bool {return taskQue_.size() < (size_t)taskQueMaxThreshHold_; }))
	{
		// 一秒钟后任务队列仍然没有空间
		std::cerr << "task queue is full, submit task fail." << std::endl;
		return Result(sp, false);
	}

	// 如果有空余，把任务放入队列中
	taskQue_.emplace(sp);
	taskSize_++;	// 原子类型变量的 ++;

	// 放入任务后，通知全局 任务队列不空 在not_Empty上进行通知
	notEmpty_.notify_all();

	// cached模式，紧急任务，小而快，增加线程数量
	if (poolMode_ == PoolMode::MODE_CACHED 
		&& taskSize_ > idleThreadSize_
		&& curThreadSize_ < threadSizeThreshHold_)
	{
		std::cout << ">>create new thread in cached" << std::endl;
		// 创建新线程对象并启动线程
		std::unique_ptr<Thread> ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
		int threadId = ptr->getId();
		threads_.emplace(threadId, std::move(ptr));
		threads_[threadId]->start();
		curThreadSize_++;
		idleThreadSize_++;		// 刚启动的线程是空闲的
	}

	return Result(sp);
}

//开启线程池
void ThreadPool::start(int initThreadSize)
{
	// 设置线程池运行状态
	isPoolRunning_ = true;

	// 记录初始线程个数
	initThreadSize_ = initThreadSize;

	// 创建线程对象
	for (int i = 0; i < initThreadSize_; i++)
	{
		// 创建线程对象, 把线程函数给到Thread线程对象,使用unique_ptr智能指针
		std::unique_ptr<Thread> ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
		int threadId = ptr->getId();
		// threads_.emplace_back(std::move(ptr));
		threads_.emplace(threadId, std::move(ptr));
	}

	// 启动所有线程
	for (int i = 0; i < initThreadSize_; i++)
	{
		threads_[i]->start();
		idleThreadSize_++;			// 记录空闲线程数量加1
		curThreadSize_++;			// 总线程数量加1
	}
}

// 定义线程函数
void ThreadPool::threadFunc(int threadid)
{
	/*
	std::cout << "begin threadFunc : " << std::this_thread::get_id() << std::endl;
	std::cout << "end threadFunc : " << std::this_thread::get_id() << std::endl;
	*/

	auto lastTime = std::chrono::high_resolution_clock().now();

	while(isPoolRunning_)
	{
		std::shared_ptr<Task> task;
		{
			// 获取锁
			std::unique_lock<std::mutex> lock(taskQueMtx_);

			std::cout << "tid: " << std::this_thread::get_id() << " is getting task..." << std::endl;

			// cached模式下可能有很多线程，但是空闲超过60秒，应该把多余线程回收
			// 当前时间 - 上一次时间 > 60s
			// 锁 + 双重判断
			while (isPoolRunning_ && taskQue_.size() == 0)
			{
				if (poolMode_ == PoolMode::MODE_CACHED)
				{
						// 每一秒钟返回一次 区分：超时返回还是有任务后返回
						// 超时返回，一秒内任务队列内仍然没有任务
					if (std::cv_status::timeout ==
						notEmpty_.wait_for(lock, std::chrono::seconds(1)))
					{
						auto now = std::chrono::high_resolution_clock().now();	// 获取当前时间
						auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);	// 计算空闲时间
						if (dur.count() >= THREAD_MAX_IDLE_TIME && curThreadSize_ > initThreadSize_)
						{
							// 把线程对象从线程列表容器中删除,如何匹配线程函数与线程对象
							threads_.erase(threadid);
							// 修改记录线程数量的值
							curThreadSize_--;
							idleThreadSize_--;
							std::cout << "tid: " << std::this_thread::get_id() << " is Over!!!" << std::endl;
							exitCond_.notify_all();
							return;		// 返回后线程结束
						}
					}
				}
				else
				{
					// 等待notEmpty
					notEmpty_.wait(lock);
				}

				// 检查唤醒原因，如果是线程池析构则回收所有线程
				//if (isPoolRunning_ == false) 
				//{
				//	// 把线程对象从线程列表容器中删除,如何匹配线程函数与线程对象
				//	threads_.erase(threadid);
				//	// 修改记录线程数量的值
				//	curThreadSize_--;
				//	idleThreadSize_--;
				//	std::cout << "tid: " << std::this_thread::get_id() << " is Over!!!" << std::endl;
				//	exitCond_.notify_all();
				//	return;		// 返回后线程结束
				//}	
			}	

			if (isPoolRunning_ == false) { break; }

			idleThreadSize_--;		// 线程拿到任务不空闲了，空闲线程数量减1

			std::cout << "tid: " << std::this_thread::get_id() << " has got a task..." << std::endl;

			// 取个任务
			task = taskQue_.front();
			taskQue_.pop();
			taskSize_--;

			// 如果仍然有剩余任务 通知其他线程消费
			if (taskQue_.size() > 0)
			{
				notEmpty_.notify_all();
			}

			//  取出任务后通知任务队列不满 通知生产者
			notFull_.notify_all();
		} // 释放锁

		// 当前线程执行任务
		if (task != NULL)
		{
			// task->run();
			task->exec();
		}

		idleThreadSize_++;	// 任务执行完毕，空闲线程数量加1
		lastTime = std::chrono::high_resolution_clock().now();	// 更新任务结束时间
	}

	// 把线程对象从线程列表容器中删除,如何匹配线程函数与线程对象
	threads_.erase(threadid);
	// 修改记录线程数量的值
	curThreadSize_--;
	idleThreadSize_--;
	std::cout << "tid: " << std::this_thread::get_id() << " is Over!!!" << std::endl;
	exitCond_.notify_all();
}

// 检查运行状态
bool ThreadPool::checkRunningState() const
{
	return isPoolRunning_;
}

///////////////Task 方法实现///////////////
void Task::exec()
{
	if (result_ != NULL)
	{
		result_->setVal(run());	// 这里发生多态调用
	}
}

void Task::setResult(Result* res)
{
	result_ = res;
}

Task::Task()
	: result_(NULL)
{}


/////////////// 线程方法实现///////////////
int Thread::generateId_ = 0;

Thread::Thread(ThreadFunc func) 
	: func_(func)
	, threadId_(generateId_++)
{}

Thread::~Thread()
{}
 
// 启动线程
void Thread::start()
{
	std::thread t(func_, threadId_);
	t.detach();	// 设置分离线程 线程对象析构后，线程函数继续执行
}

int Thread::getId() const
{
	return threadId_;
}

//////////////// Result //////////////
Result::Result(std::shared_ptr<Task> task, bool isValid)
	:task_(task)
	,isValid_(isValid)
{
	task->setResult(this);
}

Any Result::get()
{
	if (!isValid_)
	{
		return "";
	}

	sem_.wait(); // task任务没有执行完，就会阻塞用户线程
	return std::move(any_);	// 没有左值拷贝构造函数
}

void Result::setVal(Any any)
{
	any_ = std::move(any);
	sem_.post();
}

