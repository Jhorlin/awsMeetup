/**
 * Created by jhorlin.dearmas on 7/24/2014.
 */
/**
 * Created by jhorlin.dearmas on 6/53/2054.
 */
(function(exports){
    "use strict";
    exports.rosterSchema =  {
        UniqueAttributes:['email'],
        BooleanAttributes:[],
        AttributeDefinitions:[
            {
                AttributeName: 'email',
                AttributeType: 'S'
            }
//            ,
//            {
//                AttributeName: 'first',
//                AttributeType: 'S'
//            },
//            {
//                AttributeName: 'last',
//                AttributeType: 'S'
//            },
//            {
//                AttributeName: 'company',
//                AttributeType: 'S'
//            }, {
//                AttributeName: 'pic',
//                AttributeType: 'S'
//            },
//            {
//                AttributeName: 'reasons',
//                AttributeType: 'S'
//            }
        ],
        KeySchema:[
            {
                AttributeName: 'email',
                KeyType: 'HASH'
            }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5
        }
    };
}(exports));
