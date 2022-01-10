package liuzl.pojo

case class AgentBean(
                     hostName             :String,
                     ip                   :String,
                     ports                :String,
                     agentId              :String,
                     applicationName      :String,
                     serviceTypeCode      :String,
                     pid                  :String,
                     agentVersion         :String,
                     vmVersion            :String,
                     startTime            :String,
                     endTimestamp         :String,
                     endStatus            :String,
                     container            :String,
                     serverMetaData       :String,
                     jvmInfo              :String,
                     setServerMetaData    :String,
                     setJvmInfo           :String,
                     setHostname          :String,
                     setIp                :String,
                     setPorts             :String,
                     setAgentId           :String,
                     setApplicationName   :String,
                     setServiceType       :String,
                     setPid               :String,
                     setAgentVersion      :String,
                     setVmVersion         :String,
                     setStartTimestamp    :String,
                     setEndTimestamp      :String,
                     setEndStatus         :String
                    ){

}
