angular
    .module('dart.controllers.header', [])
    .controller('HeaderController', ['$scope', '$timeout', '$mdSidenav', '$mdUtil',
        function($scope, $timeout, $mdSidenav, $mdUtil) {
            $scope.sidenavOpen = $mdUtil.debounce(function(){ $mdSidenav('left').toggle() }, 200);
        }
    ])
;
