# taobao-user-behavior-analysis
 利用sql+tableau对淘宝用户行为数据进行分析
## 内容介绍
 本次数据分析的主要目的是对淘宝用户在购物流程中各个环节转化流失的情况，以及用户购物偏好和习惯进行分析，为未来销量提升提供建议。 本报告针对以下问题展开：  
   * 推送产生的高点击量商品及商品类型与高购买量的商品及商品类型的对比分析
   * 从点击浏览到购买的一系列过程中，用户转化流失流程分析  
   * 用户行为(活跃度, 商品购买量)与时间(日期, 小时)分析
   * 用户次日, 三日, 七日留存分析
   * 使用RFM分析方法对用户按价值分类  
 ## 数据介绍
 数据来自[天池](https://tianchi.aliyun.com/dataset/dataDetail?dataId=649).  
 本数据集包含了2017年11月25日至2017年12月3日之间，有行为的约一百万随机用户的所有行为. 数据包含五个字段  
 |字段名称|简介|
 |--|--|
 |user_id|用户ID|
 |item_id|商品ID|
 |category_id|商品类型ID|
 |behavior|用户行为, 有4类值. pv: 点击, buy: 购买, cart: 加入购物车, fav: 收藏|
 |timestamps|行为发生时的时间戳|
 ## 安装
 Mysql, Navicat和Tableau
 
  
 
