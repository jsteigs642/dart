angular
    .module('dart.controllers.entities')
    .controller('EventsEntityController', ['$scope', '$stateParams', '$location',
        function($scope, $stateParams, $location) {
            $scope.tableOptions = {
                filters: JSON.parse($stateParams.f || '[]'),
                onTableChange: function(page, limit, filters) {
                    $location.search('f', filters.length > 0 ? JSON.stringify(filters) : null);
                }
            };
        }
    ])
;
