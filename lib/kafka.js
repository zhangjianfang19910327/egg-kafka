'use strict';
const assert = require('assert');
const path = require('path');
const kafka = require('kafka-node');
const nodemailer = require('nodemailer');
module.exports = async (app) => {
  await createKafkaClient(app);
};
async function createKafkaClient(app) {
    const  Config =app.config.kafka;
    const transporter=nodemailer.createTransport(Config.smtpConfig);
    const client = new kafka.KafkaClient({kafkaHost: Config.prot});
    const Produce = kafka.HighLevelProducer;
    let context = app.context;
    try {
        const value=await new Promise((resolve,reject)=>{
            const producer=new Produce(client);
            producer.on('ready', function () {
                producer.createTopics(Config.producerTopic, false,(error, result) => {
                  // result is an array of any errors if a given topic could not be created 
                  console.log(error,result)
                });
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
                        ...Config.consumerTopic
                    ],
                    {
                        autoCommit: true
                    }
                ); 
                
                consumer.on('message', function (value) {
                    let mailOptions=Config.mailOptions;
                    mailOptions.html=mailOptions.html(value);
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