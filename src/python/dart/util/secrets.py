

def purge_secrets(instance, schema, secrets):
    if not schema or not instance:
        return
    for prop, subschema in schema.get('properties', {}).iteritems():
        if subschema and 'x-dart-secret' in subschema:
            secrets[prop] = instance[prop]
            del instance[prop]
        if prop in instance:
            purge_secrets(instance[prop], subschema, secrets)
