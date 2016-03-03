from dart.model.datastore import DatastoreState


def terminate_datastore(emr_engine, datastore, action):
    """
    :type emr_engine: dart.engine.emr.emr.EmrEngine
    :type datastore: dart.model.datastore.Datastore
    :type action: dart.model.action.Action
    """
    if datastore.data.args['dry_run']:
        emr_engine.dart.patch_action(action, progress=1)
        return

    emr_engine.conn.terminate_jobflow(datastore.data.extra_data['cluster_id'])
    datastore.data.state = DatastoreState.DONE
    emr_engine.dart.save_datastore(datastore)
    emr_engine.dart.patch_action(action, progress=1)
