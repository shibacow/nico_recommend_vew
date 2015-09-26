require Poison

defmodule VinfoTags do
	def start_link do
		Task.start_link(fn -> loop(%{},%{}) end)
	end
	def loop(vmap,tagmap) do
		receive do
			{:put,vid,tags} ->
        vmap = Map.put(vmap,vid,tags)
			  tagmap = Enum.reduce tags,tagmap,fn a,acc ->
				  t = a["tag"]
				  Map.update(acc,t,1,fn(v)-> v+1 end)
        end
			  loop(vmap,tagmap)
			{:normalize}->
				sz  = Map.size(vmap)
				map2 = for {k,v} <- tagmap, into: %{},do: {k,1-v*9/sz}
				loop(vmap,map2)
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
	def readfiles(fname,vpid) do
		IO.puts("fname=#{fname}")
		File.open(fname,[:read,:compressed],fn(file)-> 
			Enum.each(IO.stream(file,:line),fn(line)->
				json = Poison.Parser.parse!(line)
				vid = json["video_id"]
				tags = json["tags"]
				send vpid,{:put,vid,tags}
			end)
		end)
		fname
	end
end
defmodule GenerateTags do
	defp generateTags(path,vpid,timeout) do
		File.ls!(path) |>
			Enum.map(&(Task.async(fn -> ReadFileGenTags.readfiles(path<>"/"<>&1,vpid) end))) |>
			Enum.map(&(Task.await(&1,timeout)))
	end
	def main do
		{:ok,vpid} = VinfoTags.start_link
		path = "../videoinfo3"
		timeout=100000 #100sec
		generateTags(path,vpid,timeout)
		{:ok,tsk}=Task.start_link(fn -> loop() end)
		send vpid,{:normalize}
		send vpid,{:to_list,tsk}
		send vpid,{:size,tsk}
	end
	def loop do
		receive do
			{:to_list,:ok,results} ->  
				Enum.each(results,fn(a)->
					{k,v} = a
					if v < 0.8 do
						IO.puts("k=#{k} v=#{v}")
					end
				end)
				loop()
			{:size,:ok,sz} ->  
				IO.puts("size=#{sz}")
				loop()
		end
	end
end
