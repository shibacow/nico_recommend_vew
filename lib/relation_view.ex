require Poison

defmodule Tags do
	def start_link do
		Task.start_link(fn -> loop(%{}) end)
	end
	def loop(map) do
		receive do
			{:put,tag} ->
				loop(Map.update(map,tag,1,fn(v) -> v+1 end))
			{:keys,caller} ->
				send caller,{:keys,:ok,Map.keys(map)}
				loop(map)
			{:to_list,caller} -> 
				send caller,{:to_list,:ok,Map.to_list(map)}
				loop(map)
			{:size,caller}->
				send caller,{:size,:ok,Map.size(map)}
				loop(map)
		end
	end
end

defmodule ReadFileGenTags do
	def readfiles(fname,pid) do
		IO.puts("fname=#{fname}")
		File.open(fname,[:read,:compressed],fn(file)-> 
			Enum.each(IO.stream(file,:line),fn(line)->
				json = Poison.Parser.parse!(line)
				Enum.each(json["tags"],fn(a) ->
					tag = a["tag"]
					#IO.puts("tags=#{tag}")
					send pid,{:put,tag}
				end)
			end)
		end)
		fname
	end
end
defmodule GenerateTags do
	def main do
		{:ok,pid} = Tags.start_link
		path = "../videoinfo2"
		tlist = []
		#Enum.map(File.ls!(path),fn(x) -> 
		#	fname = "#{path}/#{x}"
		#	t = Task.async(fn -> ReadFileGenTags.readfiles(fname,pid) end)
		#  Task.await(t)
		#end)
		File.ls!(path) |>
			Enum.map(&(Task.async(fn -> ReadFileGenTags.readfiles(path<>"/"<>&1,pid) end))) |>
			Enum.map(&(Task.await(&1,1000000)))
		{:ok,tsk}=Task.start_link(fn -> loop() end)
    send pid,{:to_list,tsk}
		#{:ok,tsk}=Task.start_link(fn -> loop() end)
    send pid,{:keys,tsk}
		send pid,{:size,tsk}
		pid
	end
	def get_data(pid) do
		{:ok,tsk}=Task.start_link(fn -> loop() end)
		send pid,{:size,tsk}
	end
	
	def loop do
		receive do
			{:to_list,:ok,results} ->  
				Enum.each(results,fn(a)->
					#IO.puts tuple_size a
					if elem(a,1) > 300 do
						IO.inspect a
					end
				end)
				loop()
			{:keys,:ok,results} -> 
				IO.inspect results 
				loop()
			{:size,:ok,results} -> 
				#IO.inspect results 
				IO.puts "size = #{results}"
				#loop()
		end
	end
end
