var redis = require('redis');
var Stratum = require('stratum-pool');
var CreateRedisClient = require('./createRedisClient.js');
var async = require('async');


/*
This module deals with handling shares when in internal payment processing mode. It connects to a redis
database and inserts shares with the database structure of:

key: coin_name + ':' + block_height
value: a hash with..
        key:

 */

module.exports = function(logger, poolConfig){
    var redisConfig = poolConfig.redis;
    var coin = poolConfig.coin.name;


    var forkId = process.env.forkId;
    var logSystem = 'Pool';
    var logComponent = coin;
    var logSubCat = 'Thread ' + (parseInt(forkId) + 1);
    
    var connection = CreateRedisClient(redisConfig);
    if (redisConfig.password) {
        connection.auth(redisConfig.password);
    }
    connection.on('ready', function(){
        logger.debug(logSystem, logComponent, logSubCat, 'Share processing setup with redis (' + connection.snompEndpoint + ')');
    });
    connection.on('error', function(err){
        logger.error(logSystem, logComponent, logSubCat, 'Redis client had an error: ' + JSON.stringify(err))
    });
    connection.on('end', function(){
        logger.error(logSystem, logComponent, logSubCat, 'Connection to redis database has been ended');
    });
    connection.info(function(error, response){
        if (error){
            logger.error(logSystem, logComponent, logSubCat, 'Redis version check failed');
            return;
        }
        var parts = response.split('\r\n');
        var version;
        var versionString;
        for (var i = 0; i < parts.length; i++){
            if (parts[i].indexOf(':') !== -1){
                var valParts = parts[i].split(':');
                if (valParts[0] === 'redis_version'){
                    versionString = valParts[1];
                    version = parseFloat(versionString);
                    break;
                }
            }
        }
        if (!version){
            logger.error(logSystem, logComponent, logSubCat, 'Could not detect redis version - but be super old or broken');
        }
        else if (version < 2.6){
            logger.error(logSystem, logComponent, logSubCat, "You're using redis version " + versionString + " the minimum required version is 2.6. Follow the damn usage instructions...");
        }
    });

    var poolConfigs = JSON.parse(process.env.pools);
    var poolOptions = poolConfigs[coin];
    var processingConfig = poolOptions.paymentProcessing;
    var daemon = new Stratum.daemon.interface([processingConfig.daemon], function(severity, message){
        logger[severity](logSystem, logComponent, message);
    });

    function getReward(txHash, callback){
        daemon.cmd('gettransaction', [txHash], function (result) {
            if (!result || result[0].error) {
                callback(result[0].error, null);
                return;
            }
            if (!result[0].response
                || !result[0].response.details
                || !result[0].response.details[0]
                || !result[0].response.details[0].amount) {
                callback('No response or no details in response', null);
                return;
            }

            let fee = 0;
            if (poolOptions.rewardRecipients) {
                for (let key of Object.keys(poolOptions.rewardRecipients)) {
                    fee += +poolOptions.rewardRecipients[key];
                }
            }
            callback(null, result[0].response.details[0].amount * 100 / (100 - fee));
            return;
        });
    }

    function getDifficulty(callback){
        daemon.cmd('getdifficulty', [], function (result) {
            if (!result || result[0].error) {
                callback(result[0].error, null);
                return;
            }
            if (!result[0].response) {
                callback('No response or no details in response', null);
                return;
            }

            callback(null, result[0].response);
            return;
        });
    }

    function getShares(callback){
        var localRedisCommands = [
            ['hgetall', coin + ':shares:roundCurrent']
        ]
        connection.multi(localRedisCommands).exec(function (err, result) {
            if (err) {
                callback(err, null);
                return;
            }
            if (!result || !result[0]) {
                callback('No response or no details in response', null);
                return;
            }

            let roundDiff = 0;
            for (let key of Object.keys(result[0])) {
                roundDiff += +result[0][key];
            }
            callback(null, roundDiff);
            return;
        });

    }

    this.handleShare = function(isValidShare, isValidBlock, shareData) {
        var redisCommands = [];

        if (isValidShare) {
            redisCommands.push(['hincrbyfloat', coin + ':shares:roundCurrent', shareData.worker, shareData.difficulty]);
            redisCommands.push(['hincrby', coin + ':stats', 'validShares', 1]);
        } else {
            redisCommands.push(['hincrby', coin + ':stats', 'invalidShares', 1]);
        }

        /* Stores share diff, worker, and unique value with a score that is the timestamp. Unique value ensures it
           doesn't overwrite an existing entry, and timestamp as score lets us query shares from last X minutes to
           generate hashrate for each worker and pool. */
        var dateNow = Date.now();
        var hashrateData = [ isValidShare ? shareData.difficulty : -shareData.difficulty, shareData.worker, dateNow];
        redisCommands.push(['zadd', coin + ':hashrate', dateNow / 1000 | 0, hashrateData.join(':')]);

        if (isValidBlock){
            redisCommands.push(['rename', coin + ':shares:roundCurrent', coin + ':shares:round' + shareData.height]);
            redisCommands.push(['rename', coin + ':shares:timesCurrent', coin + ':shares:times' + shareData.height]);
            // redisCommands.push(['sadd', coin + ':blocksPending', [shareData.blockHash, shareData.txHash, shareData.height, shareData.worker, dateNow].join(':')]);
            redisCommands.push(['hincrby', coin + ':stats', 'validBlocks', 1]);

            // console.log(`Found valid ${coin} block`, shareData)
            async.parallel({
                reward: ((cb) => {
                    getReward(shareData.txHash, cb)
                }),
                difficulty: ((cb) => {
                    getDifficulty(cb)
                }),
                shares: ((cb) => {
                    getShares(cb)
                }),
            }, (err, res) => {
                // console.log('err', err)
                // console.log('res', res)
                if (err) {
                    logger.error(logSystem, logComponent, logSubCat, err);
                    redisCommands.push(['sadd', coin + ':blocksPending', [shareData.blockHash, shareData.txHash, shareData.height, shareData.worker, dateNow].join(':')]);
                } else {
                    logger.debug(logSystem, logComponent, logSubCat, `Successfully requested reward for block ${shareData.blockHash}`);
                    var blockEffort = Math.floor(res.shares / res.difficulty * Math.pow(10, 4)) / Math.pow(10, 4);
                    redisCommands.push(['sadd', coin + ':blocksPending', [shareData.blockHash, shareData.txHash, shareData.height, shareData.worker, dateNow, res.reward, blockEffort].join(':')]);
                }

                // console.log('exec next commands', redisCommands)
                connection.multi(redisCommands).exec(function (err, replies) {
                    if (err)
                        logger.error(logSystem, logComponent, logSubCat, 'Error with share processor multi ' + JSON.stringify(err));
                });
            });

        } else {
            if (shareData.blockHash) {
                redisCommands.push(['hincrby', coin + ':stats', 'invalidBlocks', 1]);
            }

            connection.multi(redisCommands).exec(function (err, replies) {
                if (err)
                    logger.error(logSystem, logComponent, logSubCat, 'Error with share processor multi ' + JSON.stringify(err));
            });
        }

    };

};
