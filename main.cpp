#include <iostream>
#include <thread>
#include "threadpool.h"
using namespace std;

class MyTask:public Task
{
public:
	MyTask()
	{
		begin_ = 1;
		end_ = 100;
	}
	MyTask(int begin, int end)
		:begin_(begin)
		,end_(end)
	{}

	Any run()
	{
		// std::cout << "tid: " << std::this_thread::get_id() << "begin." << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(3));
		int sum = 0;
		for (int i = begin_; i <= end_; i++)
		{
			sum += i;
		}
		// std::cout << "tid: " << std::this_thread::get_id() << "end." << std::endl;
		return sum;
	}
private:
	int begin_;
	int end_;
};

int main(void)
{
	{
		ThreadPool pool;
		// 设置线程池工作模式
		// MODE_CACHED：变线程数
		// MODE_FIXED：固定线程数
		pool.setMode(PoolMode::MODE_CACHED);

		pool.start(4);
		Result res1 = pool.submitTask(std::make_shared<MyTask>(1, 100000));
		Result res2 = pool.submitTask(std::make_shared<MyTask>(100001, 200000));
		Result res3 = pool.submitTask(std::make_shared<MyTask>(200001, 300000));

		int sum = res1.get().cast_<int>() + res2.get().cast_<int>() + res3.get().cast_<int>();
		std::cout << "sum = " << sum << std::endl;

	}

	int sum2 = 0;
	for (int i = 1; i <= 300000; i++)
	{
		sum2 += i;
	}
	std::cout << "sum2 = " << sum2 << std::endl;

	std::getchar();
	return 0;
}

