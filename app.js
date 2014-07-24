/**
 * Created by jhorlin.dearmas on 7/22/2014.
 */

var path = require('path');
if (!process.env.EB_NODE_COMMAND) {
    (require('node-env-file'))(path.join(__dirname, '.env'));
}

(function (module, express, bodyParser, Dynamo, schema) {
    "use strict";
    var app = express();
    app.use(express.static(path.join(__dirname, 'app')));
    app.use(bodyParser.json());
    // simple logger
    app.use(function (req, res, next) {
        console.log('%s %s', req.method, req.url);
        next();
    });
    app.listen(process.env.PORT || 80, function () {
        console.log('we are up and running on port %d', process.env.PORT || 80);
    });

    var dynamoCredentials = {
        credentials: { "accessKeyId": "AKIAJU27UJATL6JL4LTQ", "secretAccessKey": "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", "region": "us-east-1" },
        client: {
            name: 'meetUP'
        }
//        ,
//        endpoint: 'http://localhost:8000'//for testing only, comment out to save to actual dynamodb
    };

    var dynamoClient = new Dynamo(dynamoCredentials.credentials, dynamoCredentials.client.name, dynamoCredentials.endpoint);
    dynamoClient.model('roster', schema.rosterSchema);

    var rosterRounter = express.Router();
    rosterRounter.route('/members').get(
        function (req, res, next) {
            dynamoClient.roster.find({}).then(function (members) {
                members.forEach(function (member) {
                    if (member.reasons) {
                        member.reasons = member.reasons.split('|');
                    }
                });

                res.send(members);
            }, function (reason) {
                res.json(500, reason);
            });
        }
    );
    rosterRounter.route('/members/:id')
        .get(function (req, res, next) {
            var id = req.params.id;
            dynamoClient.roster.find({email: id}).then(function (response) {
                if (response && response.length > 0) {
                    var member = response[0];
                    if (member.reasons) {
                        member.reasons = member.reasons.split('|');
                    }
                    return res.json(member);
                }

                res.json({});
            }, function (reason) {
                res.json(500, reason);
            });
        })
        .post(function (req, res, next) {
            var member = {
                first: req.body.first,
                last: req.body.last,
                company: req.body.company,
                email: req.params.id,
                reasons: req.body.reasons
            };

            if (member.reasons) {
                member.reasons = member.reasons.join('|');
            }

            dynamoClient.roster.add(member).then(function () {
                res.send(204);
            }, function (reason) {
                res.json(500, reason);
            });
        })
        .put(function (req, res, next) {
            var id = req.params.id;
        })
        .delete(function (req, res, next) {
            var id = req.params.id;
        });
    app.use('/roster',rosterRounter);

    module.exports = app;

}(module, require('express'), require('body-parser'), require('./dynamo'), require('./dynamoSchema')));