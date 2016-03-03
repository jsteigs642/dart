angular
    .module('dart.controllers.navbar', [])
    .controller('NavbarController', ['$scope', '$timeout', '$mdSidenav', '$mdUtil',
        function($scope, $timeout, $mdSidenav, $mdUtil) {
            $scope.sidenavClose = function () { $mdSidenav('left').close() };
        }
    ])
;
