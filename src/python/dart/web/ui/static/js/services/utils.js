angular
    .module('dart.services.utils', [])
    .factory('dtUtils', ['$mdToast', '$window',
        function($mdToast, $window) {
            function stripSingleArrayElementNulls(obj) {
                for (var property in obj) {
                    if (obj.hasOwnProperty(property)) {
                        var value = obj[property];
                        if (Array.isArray(value) && value.length === 1 &&
                            (value[0] === null || typeof value[0] === "undefined" || value[0] === '')
                        ) {
                            obj[property] = []

                        } else if (typeof value == "object") {
                            stripSingleArrayElementNulls(value);
                        }
                    }
                }
            }
            return {
                stripSingleArrayElementNulls: function(obj) {
                    stripSingleArrayElementNulls(obj);
                    return obj
                },
                wrap: function($promise, showSuccessToast) {
                    return $promise.then(
                        function(response) {
                            if (showSuccessToast) {
                                $mdToast.show($mdToast.simple().content('Success!').position('top right').hideDelay(3000));
                            }
                            return response
                        },
                        function(error) {
                            console.error(error);
                            $mdToast.show($mdToast.simple().content('Failed :(').position('top right').hideDelay(30000));
                            return error
                        }
                    );
                },
                extend: function(o1, o2) {
                    // http://stackoverflow.com/questions/14843815/recursive-deep-extend-assign-in-underscore-js
                    var deep = function(a, b) {
                        return $window._.isObject(a) && _.isObject(b) ? _.extend(a, b, deep) : b;
                    };
                    return $window._.extend(o1, o2, deep);
                }
            }
        }])
;
