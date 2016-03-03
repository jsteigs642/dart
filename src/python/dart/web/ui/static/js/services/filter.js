angular
    .module('dart.services.filter', [])
    .factory('FilterService', ['$window', function($window) {
        function navigateSchemas(schemas, segments, callback) {
            _.each(schemas, function (sch) {
                var obj = sch.properties.data, i;
                for (i = 0 ; i < segments.length - 1; i++) {
                    var s = segments[i];
                    if (!obj) { break }
                    if (obj.type === 'object') { obj = obj.properties[s] }
                    else if (obj.type === 'array') { obj = obj.items; i-- }
                }
                if (obj && i === segments.length - 1) {
                    if (obj.type === 'array') { obj = obj.items.properties; }
                    else { obj = obj.properties }
                    if (obj) {callback(obj, i)}
                }
            });
        }
        return {
            getFilterChoices: function (searchText, schemas) {
                var _ = $window._;
                var parts = searchText.split(/ +/);
                var atOperand_1 = parts.length === 1;
                var atOperand_2 = parts.length === 3;
                var atOperator = parts.length === 2;
                var segments = parts[0].split('.');
                var choices = {};

                if (atOperator) {
                    var operators = ['=', '!=', '<=', '<', '>=', '>', 'IN', 'NOT_LIKE', 'LIKE'];
                    var chs = [];
                    _.each(operators, function (e) {
                        if (_.startsWith(e, parts[1])) {
                            chs.push(parts[0] + ' ' + e);
                        }
                    });
                    return chs;
                }

                if (atOperand_2) {
                    navigateSchemas(schemas, segments, function(last_obj, last_index) {
                        var obj = last_obj[segments[last_index]];
                        if (obj && obj.enum) {
                            var lastPart = parts[2];
                            var prefix = parts.slice(0, 2).join(' ');
                            var enumsToSkip = {};
                            if (parts[1] === 'IN') {
                                var commaParts = parts[2].split(',');
                                if (commaParts.length == 1) {
                                    prefix += ' ';
                                } else if (commaParts.length > 1) {
                                    lastPart = commaParts[commaParts.length - 1];
                                    prefix += commaParts[0] === '' ? ' ' : ' ' + commaParts.slice(0, commaParts.length - 1).join(',') + ',';
                                    _.each(commaParts, function(cp) { enumsToSkip[cp] = 1; })
                                }
                            } else {
                                prefix += ' ';
                            }
                            _.each(obj.enum, function(e) {
                                if (_.startsWith(e, lastPart)) {
                                    if (!(e in enumsToSkip)) {
                                        choices[prefix + e] = 1
                                    }
                                }
                            });
                        }
                    });
                    return _.sortBy(_.keys(choices));
                }

                if (atOperand_1) {
                    if (segments.length === 0) {
                        choices['id'] = 1;
                        choices['created'] = 1;
                        choices['updated'] = 1;
                    }
                    if (segments.length === 1) {
                        if (_.startsWith('id', segments[0])) { choices['id'] = 1 }
                        if (_.startsWith('created', segments[0])) { choices['created'] = 1 }
                        if (_.startsWith('updated', segments[0])) { choices['updated'] = 1 }
                    }
                    navigateSchemas(schemas, segments, function(last_obj, last_index) {
                        var prefix = last_index > 0 ? segments.slice(0, last_index).join('.') + '.' : '';
                        _.each(_.keys(last_obj), function (k, v) {
                            if (_.startsWith(k, segments[last_index])) {
                                choices[prefix + k] = 1;
                            }
                        });
                    });
                }

                return _.sortBy(_.keys(choices));
            }
        }
    }])
;
