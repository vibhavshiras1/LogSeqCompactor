import random
import time
import threading


class ArrayCompaction:
    def __init__(self, data_arr, frag_per) -> None:
        self.data_arr = data_arr
        self.frag_per = frag_per
        pass

    def insert_arr_continuous(self):

        while True:
            last_sub_arr = self.data_arr[-1]
            if len(last_sub_arr) < 5:
                self.data_arr[-1].append(random.randint(40, 55))
            else:
                self.data_arr.append([random.randint(40, 55)])
            time.sleep(2)
            print(self.data_arr)

    def compaction_process(self):

            index = 0
            log_num = 0
            for sub_arr_idx in range(len(self.data_arr)):
                if self.compaction_per_log[log_num] < self.frag_per:
                    index += len(self.data_arr[sub_arr_idx])

                else:
                    sub_arr_new = []
                    local_index = 0
                    for ele in self.data_arr[sub_arr_idx]:
                        if index >= self.last_index_map[ele]:
                            sub_arr_new.append(ele)
                        local_index += 1
                        index += 1

                    if len(sub_arr_new) > 0:
                        self.data_arr[sub_arr_idx] = sub_arr_new
                    else:
                        self.data_arr.remove(sub_arr_idx)

                log_num += 1
            print("Compacted log = ", self.data_arr)
            time.sleep(10)

    def compute_compaction_per(self):

        while True:
            self.last_index_map = dict()
            index = 0

            for subarr in self.data_arr:
                for ele in subarr:
                    self.last_index_map[ele] = index
                    index += 1

            index = 0
            self.compaction_per_log = []
            for subarr in self.data_arr:
                stale_count = 0
                for ele in subarr:
                    if index < self.last_index_map[ele]:
                        stale_count += 1
                    index += 1
                if len(subarr) > 0:
                    self.compaction_per_log.append(float(stale_count)/float(len(subarr)))

            print("Compaction per log segment = ", self.compaction_per_log)
            time.sleep(2)

    def change_frag_per(self, new_frag_val):

        if new_frag_val > 1 or new_frag_val <= 0:
            print("Value should be in between 0 and 1 (exclusive)")
            return 
        self.frag_per = new_frag_val
        print("New frag value has been assigned")


    def main(self):

        for i in range(5):
            sub_arr = []
            for j in range(5):
                ele = random.randint(40, 55)
                sub_arr.append(ele)
            self.data_arr.append(sub_arr)

        print(self.data_arr)
        t1 = threading.Thread(target=self.insert_arr_continuous)
        t2 = threading.Thread(target=self.compute_compaction_per)
        t3 = threading.Thread(target=self.compaction_process)
        t1.start()
        t2.start()
        t3.start()
        # print(com_per_log)
        # compacted_log = compaction_process(data, last_index_map, com_per_log, frag_per)
        # print("Compacted log = ", compacted_log)

if __name__ == "__main__":
    obj = ArrayCompaction([], 0.5)
    obj.main()




