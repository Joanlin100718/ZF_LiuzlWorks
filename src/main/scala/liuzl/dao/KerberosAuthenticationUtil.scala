package liuzl.dao

import org.apache.hadoop.security.UserGroupInformation

object KerberosAuthenticationUtil {
  def kerberosAuthentication( ): Unit = {
    try {

      //等同于把krb5.conf放在$JAVA_HOME\jre\lib\security，一般写代码即可
      System.setProperty("java.security.krb5.conf", "C:\\Users\\BigData\\Desktop\\代码配置文件\\krb5.conf")
      //      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")   // 服务器上对应的地址

      //下面的conf可以注释掉是因为在core-site.xml里有相关的配置，如果没有相关的配置，则下面的代码是必须的
      //      val conf = new Configuration
      //      conf.set("hadoop.security.authentication", "kerberos")
      //      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("hive/datahive1@CTSIBD.COM", "C:\\Users\\BigData\\Desktop\\代码配置文件\\hive.service.keytab")
//      UserGroupInformation.loginUserFromKeytab("nn/datahdfsnn1@CTSIBD.COM", "C:\\Users\\BigData\\Desktop\\nn.service.keytab")
      //      UserGroupInformation.loginUserFromKeytab("hive/datahive1@CTSIBD.COM", "/etc/security/keytabs/hive.service.keytab")  // 服务器上对应的 kerberos认证的地址

      // 打印认证信息
      println(UserGroupInformation.getCurrentUser, UserGroupInformation.getLoginUser)

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
