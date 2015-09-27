require Poison
require Logger

defmodule VinfoTags do
	def start_link do
		Task.start_link(fn -> loop(%{},%{}) end)
	end
	def loop(vmap,tagmap) do
		receive do
			{:put,vid,tags} ->
			  loop(Map.put(vmap,vid,tags),tagmap)
			{:tagput,t} ->
			  loop(vmap,Map.update(tagmap,t,1,fn(v)-> v+1 end))
			{:normalize,caller}->
			  Logger.info("normalize start")
				sz  = hd(Enum.sort(Map.to_list(tagmap),fn(a,b) -> elem(a,1) > elem(b,1) end))
			  sz = elem(sz,1) |> :math.sqrt |> :math.sqrt
			  Logger.info("sz=#{sz}")
				tagmap = for {k,v} <- tagmap, into: %{},do: {k,1-:math.sqrt(:math.sqrt(v))/sz}
				send caller,{:normalize,:ok,vmap,tagmap}
				loop(vmap,tagmap)
			{:to_list,caller} ->
				send caller,{:to_list,:ok,Map.to_list(tagmap)}
				loop(vmap,tagmap)
			{:size,caller} ->
				send caller,{:size,:ok,Map.size(tagmap)}
				loop(vmap,tagmap)
		end
	end
end

defmodule ReadFileGenTags do
	defp has_tag?(json,matchtags) do
		tags = Enum.reduce(json["tags"],[],fn(a,acc)->
			t = a["tag"]
			acc ++ [t]
		end)
		r = Enum.map(matchtags,fn(tag)->
			Enum.any?(tags,fn(x)-> String.downcase(x) == tag end)
		end)
		Enum.any?(r)
	end

	def readfiles(fname,vpid) do
		Logger.info("start fname=#{fname}")
		File.open(fname,[:read,:compressed],fn(file)-> 
			Enum.each(IO.stream(file,:line),fn(line)->
				json = Poison.Parser.parse!(line)
				if has_tag?(json,["mikumikudance","mmd"]) do
					vid = json["video_id"]
					tags = Enum.reduce(json["tags"],[],fn(a,acc)->
						cpt = :crypto.hash(:md5,a["tag"])
						acc++[cpt]
					end)
					send vpid,{:put,vid,tags}
					Enum.each(tags,fn(a) ->
						send vpid,{:tagput,a}
					end)
				end
			end)
		end)
		Logger.info("end fname=#{fname}")
		fname
	end
end

defmodule SimilaritySearch do
	def search({a,i},vmap,tagmap,sz) do
		avid = elem(a,0)
		atags = for k <- elem(a,1), do: k
		rpp = Enum.filter_map(Map.to_list(vmap),fn(x) -> elem(x,0) != avid end,fn(b)->
			bvid = elem(b,0)
			btags = for c <- elem(b,1), do: c
			s = HashSet.intersection(Enum.into(atags,HashSet.new),Enum.into(btags,HashSet.new)) |> HashSet.to_list 
			r = Enum.reduce(s,0,fn(t,acc) -> Map.get(tagmap,t,0) + acc end)
		{avid,bvid,r}
		end)
		pp = Enum.sort(rpp,fn(a,b)-> 
			{_,_,aa} = a 
			{_,_,bb} = b
			aa > bb end)
		cpp = Enum.slice(pp,0,40)
		File.open("mmd.txt",[:write,:append],fn(file)->
			Enum.each(cpp,fn(kk)->
				{a,b,c} = kk
				if c>0 do
					IO.puts(file,"#{a},#{b},#{c}")
				end
			end)
		end)
    if rem(i,100) == 0  do
			Enum.each(cpp,fn(kk)->
				{a,b,c} = kk
				if c>2 do
					Logger.info "sz=#{sz} i=#{i} a=#{a} b=#{b} c=#{c}"
				end
			end)
		end
		cpp
	end
end
defmodule GenerateTags do
	use Application
	defp generateTags(path,vpid,timeout) do
		File.ls!(path) |>
			Enum.map(&(Task.async(fn -> ReadFileGenTags.readfiles(path<>"/"<>&1,vpid) end))) |>
			Enum.map(&(Task.await(&1,timeout)))
	end
	def start(_type, _args) do
    import Supervisor.Spec, warn: false
    children = [
      # Define workers and child supervisors to be supervised
      # worker(VercheckEx.Worker, [arg1, arg2, arg3])
    ]
    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: GenerateTags.Supervisor]
    Supervisor.start_link(children, opts)
  end
	defp similarityVInfo(vmap,tagmap,timeout) do
		c = Map.to_list(vmap) |>
			Enum.with_index |>
			Enum.chunk(50)
		sz = Map.size(vmap)
		Enum.map(c,fn(k)-> 
			k |>
				Enum.map(&(Task.async(fn -> SimilaritySearch.search(&1,vmap,tagmap,sz) end))) |>
				Enum.map(&(Task.await(&1,timeout)))
		end)
	end
	def main(args) do
		{:ok,vpid} = VinfoTags.start_link
		path = "../videoinfo"
		timeout=1000000 #1000sec
		generateTags(path,vpid,timeout)
		{:ok,tsk}=Task.start_link(fn -> loop() end)
		send vpid,{:normalize,tsk}
		#send vpid,{:to_list,tsk}
		#send vpid,{:size,tsk}
		loop()
	end
	def loop do
		receive do
			{:to_list,:ok,results} ->  
				Enum.each(results,fn(a)->
					{k,v} = a
					if v < 0.5 do
						Logger.debug("k=#{k} v=#{v}")
					end
				end)
				loop()
			{:size,:ok,sz} ->
				Logger.debug("size=#{sz}")
				loop()
			{:normalize,:ok,vmap,tagmap} ->
				Logger.debug("normlaize end")
				timeout = 1000000 #1000sec
				similarityVInfo(vmap,tagmap,timeout)
				loop()
		end
	end
end
