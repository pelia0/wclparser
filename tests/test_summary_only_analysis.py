from wcl_archiver.parser.combat_state_machine import CombatStateMachine


def test_summary_only_mode_keeps_all_analytics_without_duplicate_event_streams():
    meta={'report_code':'abc','fight_id':1,'start_time':0,'end_time':1000,'map_id':9,'pulls':[{'start':0}]}
    actors=[{'id':1,'type':'Monk'},{'id':2,'type':'Boss'}]
    events=[
        {'timestamp':0,'type':'combatantinfo','sourceID':1,'specID':269,'gear':[{'id':123}],'auras':[{'ability':7}]},
        {'timestamp':100,'type':'cast','sourceID':1,'targetID':2,'abilityGameID':3,'classResources':[{'type':1,'amount':5}]},
        {'timestamp':200,'type':'damage','sourceID':1,'targetID':2,'abilityGameID':3,'amount':100,'hitType':2,'x':10,'y':20},
        {'timestamp':300,'type':'heal','sourceID':1,'targetID':1,'abilityGameID':4,'amount':10,'overheal':5},
        {'timestamp':900,'type':'death','targetID':1},
    ]
    expected,normal,replay=CombatStateMachine.analyze_fight(meta,actors,[],events)
    summary,skipped,spatial=CombatStateMachine.analyze_fight(meta,actors,[],events,include_normalized_events=False,include_replay_frames=False)
    assert summary==expected
    assert normal and replay['frames']
    assert skipped==[]
    assert spatial=={'map_id':9,'pulls':[{'start':0}],'frames':[]}
