'use strict';
const assert = require('assert');
const path = require('path');
const kafka = require('kafka-node');
const nodemailer = require('nodemailer');
module.exports = async (app) => {
    await createKafkaClient(app);
};

async function createKafkaClient(app) {
    const Config = app.config.kafka;
    const transporter = nodemailer.createTransport(Config.smtpConfig);
    const client = new kafka.KafkaClient({ kafkaHost: Config.prot });
    const Produce = kafka.HighLevelProducer;
    let ctx = app.context;
    try {
        const value = await new Promise((resolve, reject) => {
            const producer = new Produce(client);
            producer.on('ready', function () {
                producer.createTopics(Config.producerTopic, true, (error, result) => {
                    // result is an array of any errors if a given topic could not be created 
                    if (result) {
                        Object.defineProperty(ctx, "producer", {
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
                        Object.defineProperty(app, "consumer", {
                            value: consumer,
                            writable: false,
                            configurable: true,
                        });
                        app.consumer.on('message', function (value) {
                            const _mailOptions = app.config.kafka.mailOptions;
        
                            const mailOptions = Object.assign({}, _mailOptions, { html: _mailOptions.html(value) })
                            try {
                                transporter.sendMail(mailOptions, (error, info) => {
                                    if (error) {
                                        console.log("consumer:error:sendMail:" + error);
                                    }
                                    console.log("consumer:info:" + info);
                                });
                            } catch (error) {
                                console.log("consumer:error:" + error);
                            }
        
                        });
                        console.log("[egg-kafka] status OK");
                        resolve(true)
                    }
                    if (error) {
                        console.log("[egg-kafka] status dead");
                        reject(error)
                    }
                });
                
                
            });
        });
    } catch (error) {
        throw new Error(error);
    }










}