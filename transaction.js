/**
 * Created by Parag on 14/08/15.
 */

var dbConnection;
var defer = require("node-promise").defer;
var when = require("node-promise").when;
var allornone = require("node-promise").allOrNone;
var ObjectId = require('mongodb').ObjectID;

var ongoingRollback = null;


var transactions = function (detail) {
    this._tid = Math.floor((Math.random() * 10000000000) + 1);
    this._state = "pending";
    this.detail = detail;
    this.operations = [];
    this.queue = [];
    this.error = false;
}

var insertOne = function (obj, cname, doc, options, callback) {
    doc.pendingTransaction = {
        id: obj._tid,
        operation: "insert"
    };

    var deferred = defer();

    dbConnection.collection(cname).insertOne(doc, options, function (err, result) {


        if (typeof callback == 'function') {
            callback(err, result);
        }

        if (err) deferred.reject(err)
        else deferred.resolve(doc);

    });
    obj.queue.push(deferred);

    return deferred.promise;

}



var updateOne = function(obj,cname,filter,update,options,callback){

    var deferred = defer();

        dbConnection.collection(cname).findOne(filter,function(err,result){

            if(err){
                console.log("an error occoured");
            }
            else
            {
                var doc = result;

                if(update.$set == undefined ){
                    update.$set = {};
                }

                filter.pendingTransaction = {$exists: false}

                update.$set.pendingTransaction = {
                    id: obj._tid,
                    operation: "update",
                    doc:doc
                }


                dbConnection.collection(cname).updateOne(filter,update,options,function(err,result){
                    if (typeof callback == 'function') {
                        callback(err, result);
                    }
                    if (err) {
                        console.log("error updating record");
                        deferred.reject(err)
                    }
                    else deferred.resolve(doc);
                })


            }

        });

    obj.queue.push(deferred);
    return deferred.promise;
}


transactions.prototype = {

    init: function () {
        this.detail._state = this._state;
        this.detail._tid = this._tid;

        this.detail.createdAt = new Date();
        this.detail.lastModified = this.detail.createdAt;
    },
    collection: function (cname) {
        var that = this;

        return {
            insertOne: function (doc, options, callback) {
                if (typeof options == 'function') callback = options, options = {};
                options = options || {};

                that.operations.push({
                    collection: cname,
                    doc: doc,
                    options: options,
                    callback: callback,
                    type: "insertOne"
                });
                return insertOne(that, cname, doc, options, callback);
            },
            updateOne:function(filter, update, options, callback){

                if(typeof options == 'function') callback = options, options = {};
                options = options || {};

                that.operations.push({
                    collection: cname,
                    filter:filter,
                    update:update,
                    options: options,
                    callback: callback,
                    type: "updateOne"
                });

                return updateOne(that, cname, filter,update,options, callback);

            }

        }

    },

    changeTransactionStatus: function (state, tid) {
        console.log("state modifing to " + state + " tid " + tid);

        return dbConnection.collection("transaction").updateOne({_tid: tid}, {
            $set: {_state: state},
            $currentDate: {lastModified: true}
        })
    },


    removeTransaction: function (arg, operation) {

        var deferred = defer();
        var id = arg._id;
        console.log(id);

        console.log(operation.collection);

        dbConnection.collection(operation.collection).update({
            "_id": new ObjectId(id),
            "pendingTransaction.id": this._tid
        }, {$unset: {pendingTransaction: ""}}, function (err, res) {

            if (err) {
                console.log("err");
            }
            else deferred.resolve(res);
        })

        return deferred.promise;
    },
    removePendingTransactions: function (args) {

        var that = this;

        var promises = [];
        for (var i = 0; i < this.operations.length; i++) {
            promises.push(this.removeTransaction(args[i], this.operations[i]));
        }

        allornone(promises).then(function () {
            that.changeTransactionStatus("done", that._tid).then(function () {
                console.log("changed to done");
            }, function () {
                console.log("error");
            })
        })


    },
    commit: function () {
        var that = this;

        allornone(this.queue).then(function (args) {

            if(!that.error) {
                console.log(args);

                console.log("all executed");
                that.changeTransactionStatus("applied", that._tid).then(function () {
                    console.log("changed to applied");
                    that.removePendingTransactions(args);
                })

            }else {
                console.log("because of error not calling commit");
                console.log("starting auto rollback");
                that.rollback();
            }
        })

    },

    rollbackPendingCollectionsInsert: function (collection, tid) {

        var deferred = defer();

        console.log("finding collection" + collection + "tid " + tid);
        dbConnection.collection(collection).updateMany({
            "pendingTransaction.id": tid,
            "pendingTransaction.operation": "insert"
        }, {$unset: {pendingTransaction: ""}, $set: {active: false}}, function (err, result) {
            if (err) {
                console.log("error rollback");
            }
            else deferred.resolve(result);
        })

        return deferred.promise;
    },

    rollbackUpdate:function(result,collection){
        var doc = result.pendingTransaction.doc;
        var tid = result.pendingTransaction.id;
        var id = result._id;
        var deferred = defer();
        dbConnection.collection(collection).updateOne({"_id": new ObjectId(id),"pendingTransaction.id": tid},doc,function(err,res){
            if(err){
                console.log("update rollback failed for id "+id);
            }
            else {
                console.log("update rollback done for id "+id);
                deferred.resolve(res);
            }


        })
        return deferred.promise;

    },
    rollbackPendingCollectionsUpdate:function(collection,tid){
        var that = this;
        var deferred = defer();
        dbConnection.collection(collection).find({"pendingTransaction.id": tid,"pendingTransaction.operation": "update"},function(err,resultCursor){
            if(err){
                console.log("rollback find error");
            }
            else{


                that.handleAllFaultyTransactions(resultCursor, function (results) {
                    var promises = [];

                    for (var i = 0; i < results.length; i++) {
                        var result = results[i];
                        promises.push(that.rollbackUpdate(result,collection));
                    }


                    allornone(promises).then(function () {
                        console.log("rollback all updates done");
                        deferred.resolve();

                    }, function () {

                        //deferred.resolve();
                        console.log("an error occoured while rollback update");
                    });

                })



            }
        })

        return deferred.promise;

    },
    rollbackPendingTransaction: function (tx) {
        var that = this;
        var collections = tx.collections;
        var promises = [];

        for (var i = 0; i < collections.length; i++) {
            promises.push(this.rollbackPendingCollectionsInsert(collections[i], tx._tid));
            promises.push(this.rollbackPendingCollectionsUpdate(collections[i], tx._tid))
        }

        var deferred = defer();

        allornone(promises).then(function () {
            console.log("changing transaction status to failed");
            return that.changeTransactionStatus("failed", tx._tid)
        }).then(function (obj) {

            console.log("changed to failed");
            deferred.resolve();

        }, function (e) {
            console.log(e);
        })

        return deferred.promise;
    },

    handleAllFaultyTransactions: function (obj, cb) {
        var out = [];
        obj.each(function (err, item) {
            if (item == null) {
                cb(out);
            } else {
                out.push(item);
            }
        });
    },
    rollbackPending: function () {
        var that = this;

        var deferred = defer();


        dbConnection.collection("transaction").find({"_state": "pending"}, function (err, resultCursor) {
            that.handleAllFaultyTransactions(resultCursor, function (results) {

                var promises = [];


                for (var i = 0; i < results.length; i++) {
                    var result = results[i];
                    promises.push(that.rollbackPendingTransaction(result));
                }

                allornone(promises).then(function () {
                    console.log("rollback done okkkkkkk");
                    deferred.resolve();

                }, function () {

                    //deferred.resolve();
                    console.log("an error occoured while rollback");
                })
            });



        });


        ongoingRollback = deferred;
    },
    rollback: function () {
        this.rollbackPending();
    }

}


module.exports = function () {

    return {
        init: function (db) {
            dbConnection = db;

        },
        startTransaction: function (detail, cb) {

            console.log("waiting for ongoing rollback");
            var flag = false;

            if(ongoingRollback == null){
                flag = true;
                var deferred = defer();
                ongoingRollback = deferred;
            }
            console.log("here");
            when(ongoingRollback).then(function(){

                console.log("all set ready to do transaction");

                var t = new transactions(detail);
                t.init();

                dbConnection.collection("transaction").insertOne(detail, function (err, result) {
                    if (err) throw err;
                    t._cacheObj = result;

                    cb(t);
                })

            })

            if(flag)
            deferred.resolve();

        },
        rollback: function () {
            var t = new transactions({});
            t.rollback();
        }
    }

}