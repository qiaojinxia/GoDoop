package src

import "hash/fnv"

/**
 * Created by @CaomaoBoy on 2020/2/21.
 *  email:<115882934@qq.com>
 */

//一致性 Hash 算法
func ihash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() & 0x7fffffff
}
