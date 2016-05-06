
module Wheat
	let wheat pred_wheat sigma_wheat = LogNormal(pred_wheat,sigma_wheat)
	let gauss_niche env opt std = let d = (env-opt)/std in exp(-0.5*d*d)

	let wheat_airt  airt opt_airt_wheat std_airt_wheat = gauss_niche airt opt_airt_wheat std_airt_wheat
	let wheat_prate  prate opt_prate_wheat std_prate_wheat = gauss_niche prate opt_prate_wheat std_prate_wheat
	let pred_wheat (max_wheat:float) wheat_airt wheat_prate = max_wheat*wheat_airt*wheat_prate

	let opt_airt_wheat airt = let s = summary airt in if s.variance>0.0 then Uniform(1.5*s.min-0.5*s.max, 1.5*s.max-0.5*s.min) else Uniform(-1000.0, 1000.0)
	let std_airt_wheat airt = let s = summary airt in if s.variance>0.0 then LogNormal(s.max-s.min, 0.2) else LogNormal(1.0, 7.0)
	let opt_prate_wheat prate = let s = summary prate in if s.variance>0.0 then Uniform(1.5*s.min-0.5*s.max, 1.5*s.max-0.5*s.min) else Uniform(-1000.0, 1000.0)
	let std_prate_wheat prate = let s = summary prate in if s.variance>0.0 then LogNormal(s.max-s.min, 0.2) else LogNormal(1.0, 7.0)
	let max_wheat wheat = let s = summary wheat in LogNormal(s.max, 2.3)



let w = work {
	let! wheatData = importFile (ForeignContent "wheat.csv") Delimiter.Comma
	let! wheatClimate = fetchClimate wheatData fcVars fcDomain fcUri
	let! chain = estimate target wheatClimate settings
	let! pred = simulate chain wheatData
	let! chart = makeChart pred []
	return pred
}
