angular
    .module('dart.services.graph_load_modal', [])
    .factory('GraphLoadModalService', ['$mdDialog', function($mdDialog) {
        return {
            showDialog: function showDialog(ev, onSelectedItemChange) {
                $mdDialog.show({
                    controller: 'GraphLoadModalController',
                    templateUrl: 'static/partials/tools/graph_load_modal.html',
                    parent: angular.element(document.body),
                    targetEvent: ev,
                    clickOutsideToClose: true,
                    locals: {onSelectedItemChange: onSelectedItemChange},
                    bindToController: true
                })
            }
        }
    }])
;
