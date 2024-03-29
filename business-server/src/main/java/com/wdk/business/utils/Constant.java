package com.wdk.business.utils;

public class Constant {

    //************** FOR MONGODB ****************

    public static String MONGODB_DATABASE = "recommend";

    public static String MONGODB_USER_COLLECTION= "User";

    public static String MONGODB_PRODUCT_COLLECTION = "Product";

    public static String MONGODB_RATING_COLLECTION = "Rating";

    public static String MONGODB_AVERAGE_PRODUCTS_SCORE_COLLECTION = "averageProducts";

    public static String MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs";

    public static String MONGODB_RATE_MORE_PRODUCTS_COLLECTION = "RateMoreProducts";

    public static String MONGODB_RATE_MORE_PRODUCTS_RECENTLY_COLLECTION = "RateMoreRecentlyProducts";

    public static String MONGODB_STREAM_RECS_COLLECTION = "StreamRecs";

    public static String MONGODB_USER_RECS_COLLECTION = "UserRecs";

    public static String MONGODB_ITEMCF_COLLECTION = "ItemCfProductRecs";

    public static String MONGODB_CONTENTBASED_COLLECTION = "ContentProductRecs";

    //************** FOR PRODUCT RATING ******************

    public static String PRODUCT_RATING_PREFIX = "PRODUCT_RATING_PREFIX";

    //商品浏览日志埋点
    public static String PRODUCT_PV_PREFIX = "PRODUCT_PV_PREFIX";

    //用户登录日志埋点
    public static final String USER_LOGIN_PREFIX = "LOGIN_PREFIX";

    public static int REDIS_PRODUCT_RATING_QUEUE_SIZE = 40;
}
