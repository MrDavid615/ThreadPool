#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <thread>
#include <memory>	// ����ָ��
#include <atomic>	// �̰߳�ȫԭ�Ӳ���
#include <mutex>	// ������
#include <condition_variable>
#include <functional>
#include <unordered_map>

// Any ����
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
		// ����ָ��ת��������ָ��
		Derive<T>* pd = dynamic_cast<Derive<T>*>(base_.get());
		if (pd == NULL)
		{
			throw "type is unmatch!";
		}
		return pd->data_;
	}
private:
	// ��������
	class Base
	{
	public:
		virtual ~Base() = default;
	};

	// ������
	template<typename T>
	class Derive :public Base
	{
	public:
		Derive(T data):data_(data)
		{}
		T data_;
	};

private:
	// ����һ������ָ��
	std::unique_ptr<Base> base_;
};

// ʵ��һ���ź���
class Semaphore
{
public:
	Semaphore(int resLimit = 0) 
		:resLimit_(resLimit)
	{};
	~Semaphore() = default;

	// ��ȡһ���ź�����Դ
	void wait()
	{
		std::unique_lock<std::mutex> lock(mtx_);
		// ���ź�����Դ����0 ����
		cond_.wait(lock, [&]()->bool {return resLimit_ > 0; });
		// ����һ���ź���
		resLimit_--;
	}

	// ����һ���ź�����Դ
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

class Task;	// ǰ������

// ���񷵻�ֵ
class Result
{
public:
	Result(std::shared_ptr<Task> task, bool isValid = true);

	~Result() = default;

	void setVal(Any any);

	// �û����û�ȡtask��ֵ
	Any get();
private:
	Any any_;		// �洢����ķ���ֵ
	Semaphore sem_;	// �߳�ͨ�ŵ��ź���
	std::shared_ptr<Task> task_;	// ָ���Ӧ��ȡ����ֵ���������
	std::atomic_bool isValid_;	// �Ƿ��ύ�ɹ�
};

// ����������
// �û������������ͣ���Task�̳У���дrun����
class Task
{
public:
	Task();
	~Task() = default;
	void exec();
	void setResult(Result* res);

	virtual Any run() = 0;
private:
	Result* result_;	// ���ﲻ����ǿ����ָ�룬�������ã���Զ���ͷ�
};

enum class PoolMode
{
	MODE_FIXED,		// �̶������߳�
	MODE_CACHED,	// �ɶ�̬�����߳�
};

class Thread
{
public:
	using ThreadFunc = std::function<void(int)>;	// �̺߳�����������

	Thread(ThreadFunc func);

	~Thread();
	// �����߳�
	void start();

	// ��ȡ�߳�ID
	int getId() const;
private:
	ThreadFunc func_;
	static int generateId_;
	int threadId_;		// �����߳�ID
	
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

	// ���ù���ģʽ
	void setMode(PoolMode mode);

	// �޸���ֵ
	void setTaskQueMaxThreshHold(int threshhold);

	// ����Cachedģʽ���߳�����
	void setThreadSizeThreshHold(int threshhold);

	// �ύ����
	Result submitTask(std::shared_ptr<Task> sp);

	//�����̳߳�
	void start(int initThreadSize = std::thread::hardware_concurrency());

	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;

private:
	// �����̺߳��� ���ڴ������������������
	void threadFunc(int threadid);

	// ���pool������״̬
	bool checkRunningState() const;

private:
	// std::vector<std::unique_ptr<Thread>> threads_;	// �߳��б�
	std::unordered_map<int, std::unique_ptr<Thread>> threads_;	// �߳��б�2.0
	int initThreadSize_;							// ��ʼ�߳�����
	std::atomic_int curThreadSize_;					// ��¼��ǰ�߳�������
	int threadSizeThreshHold_;						// �߳�����������ֵ
	std::atomic_int idleThreadSize_;				// ��¼�����̵߳�����

	// ����û��ύ��һ���������������������п����õ�һ�������Ļ���ָ��
	// ���ú��������ڣ���Ҫʹ������ָ��
	std::queue<std::shared_ptr<Task>> taskQue_;	// ������У�ʹ������ָ��
	std::atomic_int taskSize_;					// ����������ԭ�Ӳ���
	int taskQueMaxThreshHold_;					// �����������������ֵ

	std::mutex taskQueMtx_;						// ��֤������е��̰߳�ȫ
	std::condition_variable notFull_;			// ��ʾ������в���
	std::condition_variable notEmpty_;			// ��ʾ������в���
	std::condition_variable exitCond_;			// �ȴ��߳���Դ����

	PoolMode poolMode_;							// ����ģʽ
	std::atomic_bool isPoolRunning_;			// ��ʾ�̳߳�����״̬
};

#endif // THREADPOOL_H
