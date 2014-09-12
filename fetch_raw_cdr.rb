#!/usr/bin/ruby

require './info'
require 'net/sftp'
# pate on each server
$base_path = "/cdr/work/archive/raw"
# CDR organized into folder by switches
$switches = { 	miles: 	%w[LSMSS10 LSMSS11 LSMSS30],
		barcay: %w[LSMSS31 ZHMSS20 ZHMSS21] }

infos = Info.raw_cdr

def fetch_files_from(host)
	# start SFTP session 
	Net::SFTP.start(host.to_s,infos[:user],password:infos[:pass]) do |sftp|
		count = 0
		#get files for each switch
		$switches[host].each do |switch|
			#begin
			tmp = 0
			sftp.dir.glob("#{$base_path}/#{switch}/","*.DAT") do |entry|
				tmp = tmp  + 1
			end
			count = count + tmp
			puts "#{$base_path}/#{switch}/ has #{count} cdrs."
			#rescue Net::SFTP::Operations::StatusException => e
			#	STDERR.puts "#{Time.now} : #{e.message}"
			#	abort
			#end

		end
	
		puts "==> Found #{count} raw CDRS on #{host.to_s} ..."
	end


end

def fetch_all_files
	$infos[:hosts].each do |host|
		fetch_files_from(host)	
	end
end

puts "Exit ..."
exit
