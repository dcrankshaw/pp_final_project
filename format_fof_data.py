import sys

#str(sys.argv[1])

MAX_SNAP = 63
MIN_SNAP = 0

#input formatted as follows:
#	<snapnum>, <fofID>, <ID1>, <ID2>,... <IDn>
def get_input(filename):
	formatted_input = {}
	input_file = open(filename, 'r')
	for line in input_file:
		current = format(line)
		if current is not None:
			formatted_input[current[0]] = current[1]
	input_file.close()
	return formatted_input



def format(line):
	split = line.split(", ")
	if len(split) < 3:
		return None
	snap = int(split[0])
	fofid = int(split[1])
	ids_string = set(split[2:])
	ids = frozenset([int(x) for x in ids_string])
	globalID = snap << 32	#place snapnum in the 5th byte
	globalID += fofid			#place fofID in lower 4 bytes

	#return (globalID, (int(snap), int(fofid), ids))
	return (int(fofid), ids))

def read_files():
	base = 'snap_'

	prev_input = get_input(base + str(0))
	current_input = get_input(base + str(1))
	next_input = get_input(base + str(2))
	#now have dict of form <fofID: IDfrozenset> for each snap






def make_edges(prev, current, next, current_snap):
	prev_snap = current_snap - 1
	next_snap  = current_snap + 1
	



def main():
	if len(sys.argv) is not 3:
		print "Usage: python format_fof_data.py INPUT_DIR OUTPUT_DIR"
	#groups = get_input(str(sys.argv[1]))
	print groups

if __name__ == '__main__':
	main()