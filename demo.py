import multiprocessing
from multiprocessing import Queue, Manager, Process, Pool
import time

class A:
    def func(self, x, te_list):
        print(x)
        te_list.append(x)
        # while True:
        #     if x.empty():
        #         print('*'*50)
        #         break
        #     x_ = x.get()
        #     print(x_)

    def test(self):
        new_list = Manager().list()
        start = time.time()
        pol = Pool(16)
        for i in range(10000):
            pol.apply_async(self.func, args=(i, new_list))
        pol.close()
        pol.join()
        print(len(new_list))
        print(time.time()-start)

        # for i in range(100000):
        #     queuesData.put(i)
        # new_list = Manager().list()
        # thr_list = []
        # for i in range(8):
        #     thr = Process(target=self.func, args=(queuesData, new_list))
        #     thr_list.append(thr)
        # for thr in thr_list:
        #     thr.start()
        # print('进程已全部打开')
        # for thr in thr_list:
        #     thr.join()
        # exit()

if __name__ == '__main__':
    ob = A()
    # manager = multiprocessing.Manager()
    # queuesData = manager.Queue()
    ob.test()