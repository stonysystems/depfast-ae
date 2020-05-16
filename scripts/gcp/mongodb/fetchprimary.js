db.adminCommand( { replSetGetStatus: 1 } )["members"].map((member) => {
	return {"name": member["name"], "stateStr": member["stateStr"]};
	});
