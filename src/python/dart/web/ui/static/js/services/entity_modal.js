angular
    .module('dart.services.entity_modal', [])
    .factory('EntityModalService', ['$mdDialog', function($mdDialog) {
        return {
            showDialog: function showDialog(ev, entity, getSchema, saveEntity) {
                $mdDialog.show({
                    controller: 'EntityModalController',
                    templateUrl: 'static/partials/entity_modal.html',
                    parent: angular.element(document.body),
                    targetEvent: ev,
                    clickOutsideToClose: true,
                    locals: {entity: entity, getSchema: getSchema, saveEntity: saveEntity}
                })
            }
        }
    }])
;
