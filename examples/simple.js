var winston = require('winston'),
    WinstonCloudWatch = require('../index');


// when you don't provide a name the default one
// is CloudWatch
winston.add(new WinstonCloudWatch({
  logGroupName: '/test/jp-logging',
  logStreamName: 'logging-test-1',
  awsRegion: 'us-west-2'
}));

winston.error('1');
