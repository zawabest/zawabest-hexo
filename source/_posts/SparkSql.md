---
title: SparkSql
date: 2020-01-31 23:44:45
category: Code
tags: Spark
---

* 使用spark快速求出用户点击过的商品类目
```scala
// 1) broadcast-hash join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 4*10485760)
spark.sql("select userid,category_id /*broadcast(product_info)*/ from user_click left join product_info on productid=product_id")

// 2) sort-merge join
spark.conf.set("spark.sql.join.preferSortMergeJoin", true)
spark.sql("select userid,category_id from user_click left join product_info on productid=product_id")
```

* 使用spark求出每个类目点击量最大的50个商品
```scala
spark.sql("select product_id,category_id,click from (select product_id,category_id,click,dense_rank() over (partition by category_id order by click desc) as rank from (select product_id,click,category_id from (select productid,count(userid) as click from user_click group by productid) left join product_info on productid=product_id)) where rank <=50")
```
