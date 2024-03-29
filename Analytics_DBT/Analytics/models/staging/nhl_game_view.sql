SELECT 
	f.game_facts_id,
	d.date,
	t.team,
	o.opponent,
	f.toi,
	f.cf,
	f.ca,
	f.cf_percentage,
	f.ff,
	f.fa,
	f.ff_percentage,
	f.sf,
	f.sa,
	f.sf_percentage,
	f.gf,
	f.ga,
	f.real_score,
	f.opponent_real_score,
	f.gf_percentage,
	f.xgf,
	f.xga,
	f.xgf_percentage,
	f.scf,
	f.sca,
	f.scf_percentage,
	f.hdcf,
	f.hdca,
	f.hdcf_percentage,
	f.hdsf,
	f.hdsa,
	f.hdsf_percentage,
	f.hdgf,
	f.hdga,
	f.hdgf_percentage,
	f.hdsh_percentage,
	f.hdsv_percentage,
	f.mdcf,
	f.mdca,
	f.mdcf_percentage,
	f.mdsf,
	f.mdsa,
	f.mdsf_percentage,
	f.mdgf,
	f.mdga,
	f.mdgf_percentage,
	f.mdsh_percentage,
	f.mdsv_percentage,
	f.ldcf,
	f.ldca,
	f.ldcf_percentage,
	f.ldsf,
	f.ldsa,
	f.ldsf_percentage,
	f.ldgf,
	f.ldga,
	f.ldgf_percentage,
	f.ldsh_percentage,
	f.ldsv_percentage,
	f.sh_percentage,
	f.sv_percentage,
	f.pdo,
	f.attendance
FROM {{ ref('raw_game_facts') }} AS f LEFT JOIN {{ ref('raw_dates') }} AS d
ON f.date_id = d.date_id
LEFT JOIN {{ ref('raw_teams') }} AS t
ON f.team_id = t.team_id
LEFT JOIN {{ ref('raw_opponents') }} AS o
ON f.opponent_id = o.opponent_id
