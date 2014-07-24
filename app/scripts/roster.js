(function (angular, Polymer) {
    "use strict";
    angular.module('roster', ['ngRoute'])
        .config(['$routeProvider', function ($routeProvider, Polymer) {
            $routeProvider
                .when('/', {templateUrl: 'views/roster.html', controller: 'Roster'})
                .when('/members/:id', {templateUrl: 'views/members.html', controller: 'Member'})
                .when('/404', {templateUrl: 'views/404.html'})
                .otherwise({redirectTo: '404'});
        }])
        .controller('App', ['$scope', function ($scope) {

        }])
        .controller('Roster', ['$scope', function ($scope) {
            $scope.view = 'roster';
            $scope.test = "jhorlin";
            $scope.member = {
                first: 'Jhorlin',
                last: 'De Armas',
                company: 'riptide',
                reasons: ['developer'],
                pic:undefined,
                upload:undefined,
                preview:undefined
            };

            $scope.members = [];

            $scope.addMember = function(member){
                $scope.members.unshift({
                   // first: 'Jhorlin',
                   // last: 'De Armas',
                   // company: 'riptide',
                   // email:jhorlin@gmail.com
                    reasons: [],
                    new:true
                   // pic:undefined,
                   // upload:undefined,
                   // preview:undefined
                });
            };

            $scope.save = function(member){

            };
            $scope.cancel = function(member){
              var index = $scope.members.indexOf(member);
                $scope.members.splice(index,1);
            };

            $scope.update = function(member){

            };
        }])
        .controller('Member', ['$scope', '$route', function ($scope, $route) {
            $scope.view = 'member';
            $scope.id = $route.current.params.id;
        }])
        .directive('paperInput', ['$sce', function ($sce) {
            return {
                restrict: 'E', // only activate on element attribute
                require: '?ngModel', // get a hold of NgModelController
                link: function (scope, element, attrs, ngModel) {
                    if (!ngModel) return; // do nothing if no ng-model


                    // Specify how UI should be updated
                    ngModel.$render = function () {
                        element.attr('value', ngModel.$viewValue);
                    };

                    // Listen for change events to enable binding
                    element.on('input change', function () {
                        var input = this;
                        scope.$apply(function () {
                            ngModel.$setViewValue(input.value);
                        });
                    });
                }
            };
        }])
        .directive('paperCheckedArray', [function () {
            return {
                restrict: 'A',
                link: function (scope, element, attrs) {
                    var value = scope.$eval(attrs.value) ||  attrs.value,
                        checkedArray,
                        collectionWatcher = angular.noop;
                    scope.$watch(attrs.paperCheckedArray, function (value) {
                        collectionWatcher();
                        checkedArray = value;
                        collectionWatcher = scope.$watchCollection(attrs.paperCheckedArray, update);
                    });

                    function update(values) {
                        element.attr('checked', values.indexOf(value)!== -1);
                    }

                    element.on('change', function(e){
                        var index = checkedArray.indexOf(value);
                        if(e.target.checked){
                            if(index === -1){
                                scope.$apply(function(){
                                    checkedArray.push(value);
                                });
                            }
                        } else{
                            if(index !== -1){
                                scope.$apply(function(){
                                    checkedArray.splice(index,1);
                                });

                            }
                        }
                    });
                }
            };
        }]).directive('dropZone',['$parse',function($parse){
            return {
                restrict:'A',
                link:function(scope, element, attrs){
                    var accessor = $parse(attrs.dropZone),
                        preview = $parse(attrs.dropPreview);
                    function cancel(e){
                        e.preventDefault();
                        e.stopPropagation();
                    }
                    element.on('dragover', function(e){
                        cancel(e);
                    }).on('dragleave', function(e){
                        cancel(e);
                    }).on('drop', function(e){
                        cancel(e);
                        if(e.originalEvent.dataTransfer){
                            if(e.originalEvent.dataTransfer.files.length) {
                              cancel(e);
                                var file = e.originalEvent.dataTransfer.files[0];
                                scope.$apply(function(){
                                    accessor.assign(scope, file);
                                });
                                if(preview.assign){
                                    var reader = new FileReader();
                                    reader.onload = function(event){
                                       scope.$apply(function(){
                                           preview.assign(scope, event.target.result);
                                       });
                                    };
                                }
                                reader.readAsDataURL(file);
                            }
                        }
                    });
                }
            };
        }]).factory('members', ['$http','$q',function($http, $q){
            return {
                get:function(){
                    return $http.get('/members').then(function(result){
                        if(result.status !== 200){
                            return $q.reject(result);
                        }
                        return result.data;
                    });
                }
            };
        }]).function('member',['$http','$q', function($http, $q){
            return {
                get:function(email){
                    return $http.get('/members/' + email).then(function(result){
                        if(result.status !== 200){
                            return $q.reject(result);
                        }
                        return result.data;
                    });
                },
                add:function(email, member){
                    return $http.post('/members/' + email, member).then(function(result){
                        if(result.status !== 204){
                            return $q.reject(result);
                        }
                        return result.data;
                    });
                },
                update:function(email, member){
                    return $http.post('/members/' + email, member).then(function(result){
                        if(result.status !== 204){
                            return $q.reject(result);
                        }
                        return result.data;
                    });
                }
            };
        }]);

}(this.angular, this.Polymer));