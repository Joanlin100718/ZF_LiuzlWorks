package liuzl.utils

import java.util.ResourceBundle

object GetPropertiesUtils {


    /*
    *   绑定配置文件
    *   ResourceBundle专门用于读取配置文件，所以读取时，不需要增加扩展名
    *   国际化 = I18N => Properties
    *   val config: ResourceBundle = ResourceBundle.getBundle("druid")
    *
    * */

    def getConfig( configName : String ): ResourceBundle = {
        // 获取配置文件的名称，使用getBundle()方法
        val config: ResourceBundle = ResourceBundle.getBundle(configName)
        config
    }

    def getConfigValue( configName : String , keyName : String ): String = {
        val config: ResourceBundle = ResourceBundle.getBundle(configName)
        config.getString(keyName)
    }
    
}
