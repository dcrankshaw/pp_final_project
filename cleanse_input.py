import sys
import struct

def get_file_name(path, snap, prefix, filenum):
	name = path + "/" + prefix
	if (filenum < 10):
		name += str(00) + str(snap) + "." + str(filenum)
	else:
		name += str(0) + str(snap) + "." + str(filenum)
	return name 


def process_fof(snap, input_dir, output_dir):
	id_prefix = "group_ids_"
	tab_prefix = "group_tab_"


	skip = 0
	filenumber = 0
	group_len = []
	group_offset = []
	IDs = []
	done = False
	Ntask = 0
	nGroups = 0
	totNGroups = 0
	NIds = 0
	while not done:
		tab_name = get_file_name(input_dir, snap, tab_prefix, filenumber)
		tab_file = open(tab_name, 'rb')

		nGroups = struct.unpack('<i', tab_file.read(4))[0]
		NIds = struct.unpack('<i', tab_file.read(4))[0]
		totNGroups = struct.unpack('<i', tab_file.read(4))[0]
		Ntask = struct.unpack('<i', tab_file.read(4))[0]
		
		if nGroups > 0:
			for i in xrange(skip, skip + nGroups):
				group_len.insert(i, struct.unpack('<i', tab_file.read(4))[0])

			for i in xrange(skip, skip + nGroups):
				group_offset.insert(i, struct.unpack('<i', tab_file.read(4))[0])
			
			skip += nGroups
		
		tab_file.close()
		filenumber += 1

		if filenumber == Ntask:
			done = True
	
	nGroups = 0
	totNGroups = 0
	NIds = 0
	Ntask = 0
	done = False
	filenumber = 0
	skip = 0


	while not done:
		id_name = get_file_name(input_dir, snap, id_prefix, filenumber)

		id_file = open(id_name, 'rb')

		nGroups = struct.unpack('<i', id_file.read(4))[0]
		NIds = struct.unpack('<i', id_file.read(4))[0]
		totNGroups = struct.unpack('<i', id_file.read(4))[0]
		Ntask = struct.unpack('<i', id_file.read(4))[0]

		totNIds = sum(group_len)
		max = pow(512, 3) - 1
		if NIds > 0:
			for i in xrange(skip, skip + NIds):
				IDs.insert(i, struct.unpack('<q', id_file.read(8))[0])
				IDs[i] &= (long(1L << 34) -1L)
				if IDs[i] > max:
					print 'uh oh'
				elif IDs[i] < 1000:
					print 'Yay'
			skip += NIds
		
		id_file.close()
		filenumber += 1

		if filenumber == Ntask:
			done = True
	

	output_name = output_dir + "/snap_" + str(snap)
	out_file = open(output_name, 'w')
	for i in xrange(0, totNGroups):
		if group_len[i] > 0:
			group_ids = IDs[group_offset[i]: (group_offset[i] + group_len[i])]
			global_id = (long(snap) << 32) + i
			id_str = str(global_id) + " "
			group_str = ' '.join([`int(cur)` for cur in group_ids])
			out_file.write(id_str + " " + group_str + "\n")
	out_file.close()


def main():
	for i in xrange(10, 64):
		input_dir = sys.argv[1] + "snapdir_0" + str(i)
		process_fof(i, input_dir , sys.argv[2])
		print "Processed snapshot " + str(i)
		#print input_dir

if __name__=='__main__':
	main()
















	


			
