#include "threadpool.h"
#include <functional>
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

	// �ȴ��̳߳������е��̷߳��� ��ǰ�߳�������״̬������/����ִ����
	std::unique_lock<std::mutex> lock(taskQueMtx_);

	notEmpty_.notify_all();	// ���������߳�

	exitCond_.wait(lock, [&]()->bool {return threads_.size() == 0; });
}

// ���ù���ģʽ
void ThreadPool::setMode(PoolMode mode)
{
	if (checkRunningState()) { return; }
	poolMode_ = mode;
}

// �޸���ֵ
void ThreadPool::setTaskQueMaxThreshHold(int threshhold)
{
	if (checkRunningState()) { return; }
	taskQueMaxThreshHold_ = threshhold;
}

// ����Cachedģʽ���߳�����
void ThreadPool::setThreadSizeThreshHold(int threshhold)
{
	if (checkRunningState()) { return; }
	if (poolMode_ == PoolMode::MODE_CACHED) { threadSizeThreshHold_ = threshhold; }
}

// �ύ����
Result ThreadPool::submitTask(std::shared_ptr<Task> sp)
{
	// ��ȡ��
	std::unique_lock<std::mutex> lock(taskQueMtx_);

	// �߳�ͨ�� �ȴ�������п��� wait wait_for wait_until
	//while (taskQue_.size() == taskQueMaxThreshHold_)
	//{
	//	notFull_.wait(lock);
	//}
	if (!notFull_.wait_for(lock, std::chrono::seconds(1), 
		[&]()->bool {return taskQue_.size() < (size_t)taskQueMaxThreshHold_; }))
	{
		// һ���Ӻ����������Ȼû�пռ�
		std::cerr << "task queue is full, submit task fail." << std::endl;
		return Result(sp, false);
	}

	// ����п��࣬��������������
	taskQue_.emplace(sp);
	taskSize_++;	// ԭ�����ͱ����� ++;

	// ���������֪ͨȫ�� ������в��� ��not_Empty�Ͻ���֪ͨ
	notEmpty_.notify_all();

	// cachedģʽ����������С���죬�����߳�����
	if (poolMode_ == PoolMode::MODE_CACHED 
		&& taskSize_ > idleThreadSize_
		&& curThreadSize_ < threadSizeThreshHold_)
	{
		std::cout << ">>create new thread in cached" << std::endl;
		// �������̶߳��������߳�
		std::unique_ptr<Thread> ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
		int threadId = ptr->getId();
		threads_.emplace(threadId, std::move(ptr));
		threads_[threadId]->start();
		curThreadSize_++;
		idleThreadSize_++;		// ���������߳��ǿ��е�
	}

	return Result(sp);
}

//�����̳߳�
void ThreadPool::start(int initThreadSize)
{
	// �����̳߳�����״̬
	isPoolRunning_ = true;

	// ��¼��ʼ�̸߳���
	initThreadSize_ = initThreadSize;

	// �����̶߳���
	for (int i = 0; i < initThreadSize_; i++)
	{
		// �����̶߳���, ���̺߳�������Thread�̶߳���,ʹ��unique_ptr����ָ��
		std::unique_ptr<Thread> ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
		int threadId = ptr->getId();
		// threads_.emplace_back(std::move(ptr));
		threads_.emplace(threadId, std::move(ptr));
	}

	// ���������߳�
	for (int i = 0; i < initThreadSize_; i++)
	{
		threads_[i]->start();
		idleThreadSize_++;			// ��¼�����߳�������1
		curThreadSize_++;			// ���߳�������1
	}
}

// �����̺߳���
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
			// ��ȡ��
			std::unique_lock<std::mutex> lock(taskQueMtx_);

			std::cout << "tid: " << std::this_thread::get_id() << " is getting task..." << std::endl;

			// cachedģʽ�¿����кܶ��̣߳����ǿ��г���60�룬Ӧ�ðѶ����̻߳���
			// ��ǰʱ�� - ��һ��ʱ�� > 60s
			// �� + ˫���ж�
			while (isPoolRunning_ && taskQue_.size() == 0)
			{
				if (poolMode_ == PoolMode::MODE_CACHED)
				{
						// ÿһ���ӷ���һ�� ���֣���ʱ���ػ���������󷵻�
						// ��ʱ���أ�һ���������������Ȼû������
					if (std::cv_status::timeout ==
						notEmpty_.wait_for(lock, std::chrono::seconds(1)))
					{
						auto now = std::chrono::high_resolution_clock().now();	// ��ȡ��ǰʱ��
						auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);	// �������ʱ��
						if (dur.count() >= THREAD_MAX_IDLE_TIME && curThreadSize_ > initThreadSize_)
						{
							// ���̶߳�����߳��б�������ɾ��,���ƥ���̺߳������̶߳���
							threads_.erase(threadid);
							// �޸ļ�¼�߳�������ֵ
							curThreadSize_--;
							idleThreadSize_--;
							std::cout << "tid: " << std::this_thread::get_id() << " is Over!!!" << std::endl;
							exitCond_.notify_all();
							return;		// ���غ��߳̽���
						}
					}
				}
				else
				{
					// �ȴ�notEmpty
					notEmpty_.wait(lock);
				}

				// ��黽��ԭ��������̳߳���������������߳�
				//if (isPoolRunning_ == false) 
				//{
				//	// ���̶߳�����߳��б�������ɾ��,���ƥ���̺߳������̶߳���
				//	threads_.erase(threadid);
				//	// �޸ļ�¼�߳�������ֵ
				//	curThreadSize_--;
				//	idleThreadSize_--;
				//	std::cout << "tid: " << std::this_thread::get_id() << " is Over!!!" << std::endl;
				//	exitCond_.notify_all();
				//	return;		// ���غ��߳̽���
				//}	
			}	

			if (isPoolRunning_ == false) { break; }

			idleThreadSize_--;		// �߳��õ����񲻿����ˣ������߳�������1

			std::cout << "tid: " << std::this_thread::get_id() << " has got a task..." << std::endl;

			// ȡ������
			task = taskQue_.front();
			taskQue_.pop();
			taskSize_--;

			// �����Ȼ��ʣ������ ֪ͨ�����߳�����
			if (taskQue_.size() > 0)
			{
				notEmpty_.notify_all();
			}

			//  ȡ�������֪ͨ������в��� ֪ͨ������
			notFull_.notify_all();
		} // �ͷ���

		// ��ǰ�߳�ִ������
		if (task != NULL)
		{
			// task->run();
			task->exec();
		}

		idleThreadSize_++;	// ����ִ����ϣ������߳�������1
		lastTime = std::chrono::high_resolution_clock().now();	// �����������ʱ��
	}

	// ���̶߳�����߳��б�������ɾ��,���ƥ���̺߳������̶߳���
	threads_.erase(threadid);
	// �޸ļ�¼�߳�������ֵ
	curThreadSize_--;
	idleThreadSize_--;
	std::cout << "tid: " << std::this_thread::get_id() << " is Over!!!" << std::endl;
	exitCond_.notify_all();
}

// �������״̬
bool ThreadPool::checkRunningState() const
{
	return isPoolRunning_;
}

///////////////Task ����ʵ��///////////////
void Task::exec()
{
	if (result_ != NULL)
	{
		result_->setVal(run());	// ���﷢����̬����
	}
}

void Task::setResult(Result* res)
{
	result_ = res;
}

Task::Task()
	: result_(NULL)
{}


/////////////// �̷߳���ʵ��///////////////
int Thread::generateId_ = 0;

Thread::Thread(ThreadFunc func) 
	: func_(func)
	, threadId_(generateId_++)
{}

Thread::~Thread()
{}
 
// �����߳�
void Thread::start()
{
	std::thread t(func_, threadId_);
	t.detach();	// ���÷����߳� �̶߳����������̺߳�������ִ��
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

	sem_.wait(); // task����û��ִ���꣬�ͻ������û��߳�
	return std::move(any_);	// û����ֵ�������캯��
}

void Result::setVal(Any any)
{
	any_ = std::move(any);
	sem_.post();
}

