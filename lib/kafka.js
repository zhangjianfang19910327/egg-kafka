'use strict';
const assert = require('assert');
const path = require('path');
const kafka = require('kafka-node');
const nodemailer = require('nodemailer');
const smtpConfig = {
    "host": "smtp.163.com",
    "port": 465,
    "secure": true,
    auth: {
        user: '13691460209',
        pass: 'a13141215a'
    }
}
const transporter=nodemailer.createTransport(smtpConfig);

module.exports = async (app) => {
  
  await createKafkaClient(app.config.kafka,app);
};

async function createKafkaClient(config, app) {
  
    const  Config =app.config.kafka;
   
    const client = new kafka.KafkaClient({kafkaHost: '192.168.14.200:9092'});
    const Produce = kafka.HighLevelProducer;
    let context = app.context;
    try {
        const value=await new Promise((resolve,reject)=>{
            const producer=new Produce(client);
            producer.on('ready', function () {
                // producer.createTopics(topics, (error, result) => {
                //   // result is an array of any errors if a given topic could not be created 
                //   console.log(error,result)
                // });
                console.log("[egg-kafka] status OK");
                Object.defineProperty(context, "producer", {
                    value: producer,
                    writable: false,
                    configurable: true,
                });
                const Consumer = kafka.Consumer;
                const consumer = new Consumer(
                    client,
                    [
                        { topic: 'topic1', partition: 0 }
                    ],
                    {
                        autoCommit: true
                    }
                ); 
                
                consumer.on('message', function (value) {
                    const  mailOptions = {
                        from: '"www.zjianfang.com" <13691460209@163.com>', // sender address
                        to: '2394758186@qq.com', // list of receivers
                        subject: 'www.zjianfang.com', // Subject line
                        text: '邮箱验证服务', // plain text body
                        html: '<b><a href="https://www.zjianfang.com?a='+value.value+'">www.zjianfang.com?a='+value.value+'</a></b>' // html body
                    };
                    
                    transporter.sendMail(mailOptions, (error, info) => {
                        if (error) {
                          console.log(error);
                        }
                        console.log(info);
                    });
                });
                resolve(true);
            });
        });
    } catch (error) {
        throw new Error(error);
    }
    
   
      
    
    
      
    
 
  
  
}