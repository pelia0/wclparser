from test_disk_pipeline import setup


def test_activity_log_does_not_scan_storage_or_query_database(tmp_path, monkeypatch):
    db, pipeline, client = setup(tmp_path)
    updates = []
    pipeline._packing_progress = updates.append

    def forbidden_statistics():
        raise AssertionError('Activity logging must not block network workers on disk/DB statistics')

    monkeypatch.setattr(pipeline, 'get_staging_stats', forbidden_statistics)
    try:
        pipeline._pack_log('Started downloading Report_1 (download worker #7)')
        assert len(updates) == 1
        assert updates[0]['log'] == 'Started downloading Report_1 (download worker #7)'
    finally:
        pipeline.close()
