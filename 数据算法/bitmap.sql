BitMap的基本原理 用一个bit来标记某个元素对应的Value，而key即是该元素. 由于采用一个bit来存储一个数据. 因此可以打打的节省空间。


select bitmapBuild([1,4,5,6]) as res1, -- 创建位图对象1
       bitmapBuild([5, 3, 9, 9]) as res2,  -- 创建位图对象1

       bitmapToArray(bitmapAnd(res1, res2 )) as union_res,  --位图对象进行与操作 {5}
       bitmapToArray(bitmapOr(res1, res2)) as unionall_res, --位图对象进行异操作 {1,4,5,6,3,9}
       bitmapToArray(bitmapXor(res1, res2)) as no_union_res, --两个位图对象进行异或操作 {1,3,4,6,9}
       bitmapToArray(bitmapAndnot(res1, res2)) as res_dd,  --两个位图的差异 {1,4,6}

       bitmapCardinality(res2) as len_res2,  --位图对象的基数 3
       bitmapAndCardinality(res1, res2) as len_res3, --两个位图对象进行与操作，返回结果位图的基数 1
       bitmapOrCardinality(res1, res2) as len_res4, --两个位图进行或运算，返回结果位图的基数 6
       bitmapXorCardinality(res1, res2) as len_res5, --两个位图进行异或运算，返回结果位图的基数
       bitmapAndnotCardinality(res1, res2) as len_res6, --两个位图的差异运算 返回结果位图的基数

       bitmapContains(res1, toUInt32(111)) as has_ele  --检查位图是否包含指定元素


应用场景:
 1. 用户标签存储                                    
