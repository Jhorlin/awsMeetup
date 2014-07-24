/**
 * Created by jhorlin.dearmas on 6/13/2014.
 */
(function (module, process, dop, AWS, q, extend) {
    "use strict";

    var ACTIVE_TABLE_TIMEOUT = 10 * 1000; //set the retry timeout to 10 seconds
    var ACTIVE_TABLE_RETRY_COUNT = 10;
    var BACK_OFF_RETRY = 10;

    /**
     * Exponential backoff function
     * @param n
     * @returns {number}
     */
    function exponentialRetry(n) {
        return Math.pow(2, n) * 100;
    }

    /**
     * Converts an item into a buffer of that item
     * @param item
     * @returns {Buffer}
     */
    function toBuffer(item) {
        return new Buffer(item);
    }

    /**
     * self explanatory
     * @param item
     * @returns {*}
     */
    function toString(item) {
        return item.toString();
    }

    /**
     * Coverts a buffer back into json
     * @param item
     * @returns {Object|string|*}
     */
    function bufferToJSON(item) {
        return (new Buffer(item)).toJSON();
    }

    /**
     * Converts an proeperty value into the dialect that dynamo needs
     * Example: {name:"jhorlin"} wold be converted to {"name":{'S':"jhorlin}}
     * @param field
     * @returns {{}}
     */
    function dynamoType(field) {
        var fieldObject = {},
            isArray = field instanceof Array,
            fieldType = isArray ? typeof field[0] : typeof field;
        switch (fieldType) {
            case 'object':
            {
                fieldObject[isArray ? 'BS' : 'B'] = isArray ? field.map(toBuffer) : toBuffer(field);
            }
                break;
            case 'number':
            {
                fieldObject[isArray ? 'NS' : 'N'] = isArray ? field.map(toString) : toString(field);
            }
                break;
            case 'boolean':
            {
                fieldObject[isArray ? 'SS' : 'S'] = isArray ? field.map(toString) : toString(field);
            }
                break;
            case 'string':
            {
                fieldObject[isArray ? 'SS' : 'S'] = field;
            }
                break;
            default :
            {
                throw new Error('cannot convert type for dynamo:' + fieldType);
            }
        }
        return fieldObject;
    }

    /**
     * Converts items from a dynamo dialect field back into javascript object notation
     * * Example: {"name":{'S':"jhorlin}} wold be converted to {name:"jhorlin"}
     * @param field
     * @param typeMap
     * @returns {*}
     */
    function objectType(field, typeMap) {
        var fieldType = Object.keys(field)[0],
            ret;
        switch (fieldType) {
            case 'B':
            {
                ret = bufferToJSON(field[fieldType]);
            }
                break;
            case 'BS':
            {
                ret = field[fieldType].map(bufferToJSON);
            }
                break;
            case 'N':
            {
                ret = parseFloat(field[fieldType]);
            }
                break;
            case 'NS':
            {
                ret = field[fieldType].map(parseFloat);
            }
                break;
            case 'S':
            case 'SS':
            {
                ret = field[fieldType];
            }
                break;
            default :
            {
                throw new Error('invalid dynamo type:' + fieldType);
            }
        }
        if (typeMap) {
            ret = ret instanceof Array ? ret.map(typeMap) : typeMap(ret);
        }

        return ret;
    }

    /**
     * Converts all the properties into Dynamo Dialect properties so that we can store them in dynamoDB
     * @param item
     * @returns {{}}
     */
    function toDynamoRequest(item) {
        var request = {};
        Object.keys(item).forEach(function (key) {
            request[key] = dynamoType(item[key]);
        });
        return request;
    }

    /**
     * Converts items from dialect object notation back into javascript object notation
     * @param item
     * @param map
     * @returns {{}}
     */
    function fromDynamoResponse(item, map) {
        var request = {};
        Object.keys(item).forEach(function (key) {
            request[key] = objectType(item[key], map[key]);
        });
        return request;
    }


    /**
     * Dynamo constructor used to contain models and the db connection
     * @param connection
     * @param client
     * @constructor
     */
    function Dynamo(connection, client, endpoint) {
        this.client = client;
        this.db = connection instanceof AWS.DynamoDB ? connection : new AWS.DynamoDB(connection);
        if(endpoint){
            this.db.setEndpoint(endpoint);
        }
    }

    /**
     * Gets the attributes of a table. If the table does not exist it will be created.
     * The promise will also not be resolved until the table is in an active state.
     * @param tableName
     * @param schema
     * @param db
     * @param count
     * @returns {promise|OrmPgDbImpl.promise|Q.promise}
     */
    function activateTable(tableName, schema, db, count) {
        var deferred = q.defer();
        process.nextTick(function () {
            //if we are over our ACTIVE_TABLE_RETRY_COUNT limit just give it up;
            if (count && count > ACTIVE_TABLE_RETRY_COUNT) {
                deferred.reject(new Error("activeTable exceeded retry count of:" + ACTIVE_TABLE_RETRY_COUNT));
            }
            var tableParam = {TableName: tableName},
                tableSchema = extend(true, {}, schema, tableParam);
            db.describeTable(tableParam, function (err, data) {
                if (err) {
                    db.createTable(tableSchema, function (err, data) {
                        if (err) {
                            deferred.reject(err);
                        } else {
                            if (data.TableDescription.TableStatus === "ACTIVE") {
                                deferred.resolve(data.TableDescription);
                            }
                            else {
                                //we might be creating the table so try again in ACTIVE_TABLE_TIMEOUT milliseconds
                                setTimeout(function () {
                                    deferred.resolve(activateTable(tableName, schema, db, count + 1));
                                }, ACTIVE_TABLE_TIMEOUT);
                            }
                        }
                    });
                } else {
                    if (data.Table.TableStatus === "ACTIVE") {
                        deferred.resolve(data.Table);
                    } else {
                        setTimeout(function () {
                            //we might be creating the table so try again in ACTIVE_TABLE_TIMEOUT milliseconds
                            deferred.resolve(activateTable(tableName, schema, db, count + 1));
                        }, ACTIVE_TABLE_TIMEOUT);
                    }
                }
            });
        });

        return deferred.promise;
    }


    var batch = {};
    ['add', 'remove'].forEach(
        /**
         * Helper function. for each name in the array we create a batch for that method.
         * This function is for spitting an array if items into individual calls and track the items that fail and pass
         * If the items fail to due throuput limitations it will use exponential backoff to try to add the messages
         * in less frequency.
         * @param name
         */
            function (name) {
            batch[name] = function create(model, items, count) {
                //do a single putItem for every item in the array then wait for all items to be resolved or rejected
                return q.allSettled(items.map(model[name].bind(model))).then(function (results) {
                    var failedItems = [],
                        succesfullItems = [],
                        reject;
                    results.forEach(function (result) {
                        if (result.state === "fulfilled") {
                            succesfullItems.push(result.value);
                        } else {
                            failedItems.push(result.reason.item);
                            if (result.reason.code !== "ProvisionedThroughputExceededException") {
                                reject = reject || result.reason;
                            }
                        }
                    });
                    if (reject) {
                        reject.failed = failedItems;
                        reject.succedded = succesfullItems;
                        return q.reject(reject);
                    }
                    //if we have throttled items try again
                    if (failedItems.length === 0) {
                        return succesfullItems;
                        //if we have tried too many times just quit
                    } else if (count > BACK_OFF_RETRY) {
                        var error = new Error("Unable to" + name + "all items after max tries:" + BACK_OFF_RETRY);
                        error.failed = failedItems;
                        error.succedded = succesfullItems;
                        return q.reject(error);
                    } else {
                        var deferred = q.defer();
                        //set a timeout so that we slow our write into Dynamo
                        setTimeout(function () {
                            create(model, failedItems, count + 1).then(function (result) {
                                deferred.resolve(succesfullItems.concat(result));
                            }, function (reject) {
                                //we need to aggregate all of the succesfull items do not aggregate the failed since we push the failed items
                                // as the values into the recursive call
                                reject.succedded = succesfullItems.concat(reject.succedded);
                                deferred.reject(reject);
                            });
                            //set the timeout to an exponent of the try
                        }, exponentialRetry(count));
                        return deferred.promise;

                    }
                });
            };
        });

    /**
     * Convert a string "true" | "false" into a boolean value
     * @param str
     * @returns {boolean}
     */
    function stringToBoolean(str) {
        return str === "true";
    }

    /**
     * Model constructor that helps manages tables
     * @param tableName
     * @param schema
     * @param db
     * @param translate
     * @constructor
     */
    function Model(tableName, schema, db, translate) {
        this.unique = schema.UniqueAttributes;
        this.fieldMap = {};
        if (schema.BooleanAttributes) {
            schema.BooleanAttributes.forEach(function (attribute) {
                this.fieldMap[attribute] = stringToBoolean;
            }.bind(this));
            delete schema.BooleanAttributes;
        }
        delete schema.UniqueAttributes;
        this.table = activateTable(tableName, schema, db, 0);
        this.indeciesFieldMap = itemIndexMap([
            {KeySchema: schema.KeySchema}
        ]).concat(itemIndexMap(schema.GlobalSecondaryIndexes)).concat(itemIndexMap(schema.LocalSecondaryIndexes));
        this.translate = translate;
        this.db = db;
    }

    /**
     * When quering a dynamo on a secondary index only you need to specify the index. This function
     * allows you to pass in a query and if the index you specified in the query is not specified in the params
     * this will set the correct index
     * @param indices
     * @returns {Array}
     */
    function itemIndexMap(indices) {
        if (!indices) {
            return;
        }
        var itemsMap = [];
        indices.forEach(function (index) {
            if(index.KeySchema){
                index.KeySchema.forEach(function (schema) {
                    if (schema.KeyType === "HASH") {
                        itemsMap.push({index: index.IndexName || 'HashKey',
                            field: schema.AttributeName});
                    }
                });
            }

        });
        return itemsMap;
    }

    /**
     * Add a model (table) to dynamo
     * @param table
     * @param schema
     * @param transform
     */
    Dynamo.prototype.model = function (table, schema, transform) {
        this[table] = new Model(this.client + table, schema, this.db, transform);
    };


    /**
     * Add an Item to your dynamo table
     * @param item
     * @returns {promise}
     */
    Model.prototype.add = function (item) {
        if (item instanceof Array) {
            return batch.add(this, item, 0);
        } else {
            return this.table.then(function (table) {
                var translation = (this.translate && this.translate.toDynamo) ? this.translate.toDynamo : undefined,
                    deferred = q.defer(),
                    flatItem = toDynamoRequest(translation ? translation(dop.untranspose(item), item) : dop.untranspose(item)),
                    params = {TableName: table.TableName, Item: flatItem};
                if (this.unique) {
                    params.Expected = {};
                    //params.ConditionalOperator = 'AND';
                    this.unique.forEach(function (attr) {
                        params.Expected[attr] = {
                            //Value:params.Item[attr],
                            //AttributeValueList:[params.Item[attr]],
                            Exists: false
                            //ComparisonOperator:'NE'
                        };
                    });
                }
                this.db.putItem(params, function (err, data) {
                    if (err) {
                        err.item = item;
                        return deferred.reject(err);
                    }
                    return deferred.resolve(item);
                });
                return deferred.promise;
            }.bind(this));
        }
    };

    /**
     * Remove an item from a dynamo table
     * @param item
     * @returns {promise}
     */
    Model.prototype.remove = function (item) {
        if (item instanceof Array) {
            return batch.remove(this, item, 0);
        } else {
            return this.table.then(function (table) {
                var removeItem = {};
                if (this.unique) {
                    this.unique.forEach(function (value) {
                        removeItem[value] = item[value];
                    });
                } else {
                    removeItem = item;
                }
                var deferred = q.defer(),
                    params = {TableName: table.TableName, Key: toDynamoRequest(dop.untranspose(removeItem))};
                this.db.deleteItem(params, function (err, data) {
                    if (err) {
                        err.item = item;
                        deferred.reject(err);
                    } else {
                        deferred.resolve(item);
                    }
                });
                return deferred.promise;
            }.bind(this));
        }
    };

    /**
     * Update an item from a dynamo table
     * TODO:implement me!!!
     * @param qyery
     * @returns {promise|OrmPgDbImpl.promise|Q.promise}
     */
    Model.prototype.update = function (qyery) {
        var deferred = q.defer();
        this.table.then(function (table) {

        }, function (err) {
            return deferred.reject(err);
        });
        return deferred ? deferred.promise : undefined;
    };


    /**
     * Query a dynamo table
     * @param query
     * @param assending
     * @param startKey
     * @returns {promise}
     */
    Model.prototype.find = function (query, assending, startKey, limit) {
        return this.table.then(function (table) {
            var dynamoQuery = {},
                translation = (this.translate && this.translate.fromDynamo) ? this.translate.fromDynamo : undefined,
                deferred = q.defer();
            dynamoQuery.TableName = table.TableName;

            // Populate query object
            this.indeciesFieldMap.forEach(function (fieldAttribute) {
                if (fieldAttribute && fieldAttribute.field in query) {
                    ('KeyConditions' in dynamoQuery ? dynamoQuery.KeyConditions : (dynamoQuery.KeyConditions = {}))[fieldAttribute.field] = {
                        ComparisonOperator: 'EQ',
                        AttributeValueList: [dynamoType(query[fieldAttribute.field])]
                    };
                }
            });

            // If key conditions are found this implies a dynamodb query search
            // so add additional query filters to the results.
            if (dynamoQuery.KeyConditions) {
                Object.keys(query).forEach(function (queryKey) {
                    if (dynamoQuery.KeyConditions && queryKey in dynamoQuery.KeyConditions) {
                        return;
                    } else {
                        ('QueryFilter' in dynamoQuery ? dynamoQuery.QueryFilter : (dynamoQuery.QueryFilter = {}))[queryKey] = query[queryKey];
                    }
                });
            } else {
                // Create a scan filter to try to get the results.
                Object.keys(query).forEach(function (queryKey) {
                    ('ScanFilter' in dynamoQuery ? dynamoQuery.ScanFilter : (dynamoQuery.ScanFilter = {}))[queryKey] = query[queryKey];
                });
            }


            if (startKey) {
                dynamoQuery.ExclusiveStartKey = startKey;
            }
            this.indeciesFieldMap.every(function (map) {
                if (map && map.field in query) {
                    if (map.index !== 'HashKey') {
                        dynamoQuery.IndexName = map.index;
                    }
                    return false;
                }
                return true;
            });

            // Apply query size limit
            if (limit > 0) {
                dynamoQuery.Limit = limit;
            }

            // If key conditions are found, perform query, else perform scan
            var filterType;
            if ('KeyConditions' in dynamoQuery) {
                filterType = 'query';
            } else {
                filterType = 'scan';
            }

            this.db[filterType](dynamoQuery, function (err, data) {
                if (err) {
                    return deferred.reject(err);
                }

                var items = data.Items.map(function (item) {
                    var flat = fromDynamoResponse(item, this.fieldMap);
                    var object = dop.transpose(flat);
                    if (translation) {
                        translation(object._data, flat);
                    }
                    return object._data;
                }.bind(this));

                if (data.LastEvaluatedKey && (!limit || limit > data.Items.length)) {
                    deferred.resolve(this.find(query, assending, data.LastEvaluatedKey, limit - data.Items.length).then(function (nextItems) {
                        return items.concat(nextItems);
                    }, function (err) {
                        return q.reject(err);
                    }));
                } else {
                    deferred.resolve(items);
                }

            }.bind(this));

            return deferred.promise;
        }.bind(this));
    };


    module.exports = Dynamo;

    /**
     * Export our model
     * @param db
     * @param client
     * @param logger
     * @returns {Dynamo}
     */

//    module.exports = function (db, client, logger) {
//        var Models = new Dynamo(db, client);
//        Models.model('statement', schema.statementSchema, {
//            toDynamo: function (dynamo, object) {
//                var identifier = object.actor.getIdentifier();
//                dynamo.actorInverseIdentifier = typeof identifier === "object" ? JSON.stringify(identifier) : identifier;
//                return dynamo;
//            }, fromDynamo: function (object, dynamo) {
//                delete object.actorInverseIdentifier;
//                return object;
//            }});
//        return Models;
//    };
}(module, process, require('dataobject-parser'), require('aws-sdk'), require('q'), require('extend')));
