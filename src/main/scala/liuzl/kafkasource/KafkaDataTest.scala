package liuzl.kafkasource

import com.alibaba.fastjson.JSON

import scala.util.parsing.json.JSONObject

object KafkaDataTest {
  def main(args: Array[String]): Unit = {
    val str = "{\"data\":[{\"packageName\":\"com.ctsi.safe.appstore\",\"appName\":\"应用市场\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.smile.gifmaker\",\"appName\":\"快手\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"me.ele\",\"appName\":\"饿了么\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"cn.wps.moffice_eng\",\"appName\":\"WPSOffice\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.autonavi.minimap\",\"appName\":\"高德地图\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.baidu.searchbox\",\"appName\":\"百度\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.dragon.read\",\"appName\":\"番茄免费小说\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.eg.android.AlipayGphone\",\"appName\":\"支付宝\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.hihonor.android.honorsuite\",\"appName\":\"手机助理\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.huawei.hisuite\",\"appName\":\"华为手机助手\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.huawei.welinknow\",\"appName\":\"LinkNow\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.jingdong.app.mall\",\"appName\":\"京东\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.sina.news\",\"appName\":\"新浪新闻\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.sina.weibo\",\"appName\":\"微博\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.ss.android.article.news\",\"appName\":\"今日头条\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.taobao.litetao\",\"appName\":\"淘特\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.taobao.taobao\",\"appName\":\"淘宝\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.tencent.mtt\",\"appName\":\"QQ浏览器\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.tencent.qqlive\",\"appName\":\"腾讯视频\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.transfer.hisuite\",\"appName\":\"手机助手Suite\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.wuba\",\"appName\":\"58同城\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.ximalaya.ting.android\",\"appName\":\"喜马拉雅\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"com.zhihu.android\",\"appName\":\"知乎\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"},{\"packageName\":\"ctrip.android.view\",\"appName\":\"携程旅行\",\"wifiFlow\":0,\"mobileFlow\":0,\"collectDate\":\"2022-03-02\"}],\"deviceUuid\":\"7549d8d3-07b3-4ea0-afd5-e880b63df571\",\"uuid\":\"23cb8e5a-e5fb-4706-900a-e7b761d22783\",\"time\":1646300289559}"


    val resJson = JSON.parseObject(str)

    val deviceUuid = resJson.getString("deviceUuid")
    val data = resJson.getString("data")
    val uuid = resJson.getString("uuid")
    val time = resJson.getString("time")

    val list = JSON.parseArray(data)
    for (i <- 0 to list.size() -1 ) {
      println(list.getJSONObject(i))
    }



  }

}
