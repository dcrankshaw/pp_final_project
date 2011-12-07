using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Windows.Forms;
using BloomFilter;
namespace GadgetLoader
{
    class Runner
    {
        public bool isProcessing;
        public string SQLCommandsFile;
        private List<Process> processes = new List<Process>();
        public Runner(string _SQLCommandsFile)
        {
            this.isProcessing = false;
            this.SQLCommandsFile = _SQLCommandsFile;
        }

        public void Add(Process process)
        {
            if (process == null)
                return;
            processes.Add(process);
            process.runner = this;
        }
        public void Run()
        {
            try
            {
                isProcessing = true;
                foreach (Process process in processes)
                {
                    if (isProcessing)
                        process.Run();
                    else
                        break;
                }
                DebugOut.PrintLine("Processing complete.");
                DebugOut.PrintLine("SQL bulk insert commands saved to " + SQLCommandsFile);
                if (SQLCommandsFile != null)
                    DebugOut.SaveCommands(SQLCommandsFile);

            }
            catch (Exception e)
            {
                isProcessing = false;
                throw e;
            }
            isProcessing = false;

        }
    }



    class SnapshotsProcess : Process
    {
        public String snapshotFilePrefix;
        public int firstSnapshotFile = 0;

         
        public int lastSnapshotFile = -1;
        
        //public int lastSnapshotFile = 3; //For testing...
        public string snapshotTable = "Snapshot";
        public short snapNumber = -1;
        public bool useHsml = false;
        public bool writeArrays = true;
        public Int16 firstSnap = 0;
        public Int16 lastSnap = 0;

        

        public SnapshotsProcess(GlobalParameters globals)
            : base(globals)
        { }

        public override void Run()
        {
            if (snapNumber >= 0)
            {
                if (isProcessing)
                {
                    if (writeArrays)
                    {
                        ProcessSnapshotWithArrays(snapNumber);
                        //Console.WriteLine("Processed Arrays");
                    }
                    else
                    {
                        ProcessSnapshot(snapNumber);
                    }
                }
            }
            else
            {
                for (Int16 i = firstSnap; i <= lastSnap; i++)
                {
                    if (isProcessing)
                    {
                        if (writeArrays)
                            ProcessSnapshotWithArrays(i);
                        else
                            ProcessSnapshot(i);
                    }
                    else
                    {
                        DebugOut.PrintLine("Processing snapshots Interrupted at snapshot " + i);
                    }
                }
            }
        }


        private void ProcessSnapshotWithArrays(short snap)
        {
            //DebugOut.PrintLine("PROCESSING SNAPSHOT WITH ARRAYS" + snap);
            // using this only if needed

            List<SnapFileSummary> results = new List<SnapFileSummary>();
            //and make the directory, just to be safe
            try
            {
                Directory.CreateDirectory(outPath);

            }
            catch (IOException e)
            {
                globals.summary.addError(e.Message);
                return;
            }

            int curFile = firstSnapshotFile; // index of current read/write file
            int numFile = 0;

            if (lastSnapshotFile < firstSnapshotFile)
            {
                lastSnapshotFile = firstSnapshotFile;
                numFile = -1;
            }

            while (curFile <= lastSnapshotFile)
            {
                //DebugOut.PrintLine("..file " + curFile + "/" + lastSnapshotFile);
                Console.WriteLine("..file " + curFile + "/" + lastSnapshotFile);
                SnapFileSummary currentFileSummary = new SnapFileSummary();
                results.Add(currentFileSummary);
                currentFileSummary.start = DateTime.Now;
                currentFileSummary.fileNumber = curFile;

                string filename = "";
                try
                {
                    filename = GetSnapFile(inPath, snap, snapshotFilePrefix, curFile);
                    currentFileSummary.inFileName = filename;
                }
                
                catch (Exception e)
                {
                    currentFileSummary.badStatus = true;
                    currentFileSummary.statusMessage = e.Message;
                    globals.summary.setFileSummaries(results);
                    return;
                }
                // now load the file
                try
                {
                    SnapFile curSnap = new SnapFile(filename);
                    // and open the stream for writing
                    using (SqlBinaryWriter binwriter = new SqlBinaryWriter(new FileStream(GetSnapDefault(outPath, snap, snapshotFilePrefix, curFile), FileMode.Create)))
                    {
                        Structs[] parts = new Structs[curSnap.numSample];
                        //record destination path in file summary
                        currentFileSummary.outFileName = GetSnapDefault(outPath, snap, snapshotFilePrefix, curFile);
                        // now write each particle into the array
                        for (int i = 0; i < curSnap.numSample; i++)
                        {
                            parts[i].x = curSnap.pos[i, 0];
                            parts[i].y = curSnap.pos[i, 1];
                            parts[i].z = curSnap.pos[i, 2];
                            parts[i].vx = curSnap.vel[i, 0];
                            parts[i].vy = curSnap.vel[i, 1];
                            parts[i].vz = curSnap.vel[i, 2];
                            parts[i].snapnum = snap;
                            parts[i].id = curSnap.id[i];
                            // add in highest-order bit
                            parts[i].id |= ((UInt64)curSnap.nLargeSims[1]) << 32;
                            // make ph-key
                            parts[i].phkey = GetPHKey(parts[i].x, parts[i].y, parts[i].z);


                        }
                        // now sort before writing files
                        // TODO
                        //TBD this may not be necessary if the particles 
                        // are sorted in the files already.
                        Array.Sort<Structs>(parts, new ParticleComparator());

                        Cell cell = new Cell(snap);
                        int currentPHkey = -1;
                        for (int i = 0; i < curSnap.numSample; i++)
                        {
                            if (parts[i].phkey != currentPHkey)
                            {
                                if (cell.Count > 0)
                                    binwriter.WriteCell(cell);
                                currentPHkey = parts[i].phkey;
                                cell.Init(currentPHkey);
                            }
                            cell.AddToCell(parts[i]);
                        }
                        if (cell.Count > 0)
                            binwriter.WriteCell(cell);


                        // and add a bulk insert
                        //DebugOut.AddCommand(GetSnapDefault(outPath, snap, snapshotFilePrefix, curFile), snapshotTable);
                        Console.WriteLine("..wrote " + curSnap.numSample + "/" + curSnap.numTotals[1] + " points");
                        DebugOut.PrintLine("..wrote " + curSnap.numSample + "/" + curSnap.numTotals[1] + " points");
                    }
               

                    // and set numFiles
                    if (numFile == -1)
                    {
                        numFile = (int)curSnap.numSubfiles - firstSnapshotFile;
                        lastSnapshotFile = (int)curSnap.numSubfiles - 1;
                    }

                    currentFileSummary.end = DateTime.Now;
                    currentFileSummary.duration = results[curFile].end - results[curFile].start;
                    currentFileSummary.numParticles = curSnap.numSample;

                    if (curSnap.numSample < (int)(0.01 * LoaderConstants.particlesPerSnap))
                    {
                        currentFileSummary.warning = true;
                        currentFileSummary.warningMessage = "Less than 1% of particles in snapshot in this file";
                    }
                    if (curSnap.numSample > LoaderConstants.particlesPerSnap)
                    {
                        currentFileSummary.warning = true;
                        currentFileSummary.warningMessage = "More particles in file than are supposed to be in snapshot";
                    }

                    if(currentFileSummary.outFileName != null)
                    {
                        globals.summary.AddSnapBCPCommand(currentFileSummary.outFileName);
                    }
                    curFile++;
                    // avoid outofmemory errors
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
                catch (Exception e)
                {
                    currentFileSummary.badStatus = true;
                    currentFileSummary.statusMessage = e.Message;
                    globals.summary.setFileSummaries(results);
                    return;
                }

            }
            globals.summary.setFileSummaries(results);

        }

        private void ProcessSnapshot(short snap)
        {
            DebugOut.PrintLine("PROCESSING SNAPSHOT WITHOUT ARRAYS " + snap);
            // using this only if needed

            List<SnapFileSummary> results = new List<SnapFileSummary>();

            // and make the directory, just to be safe
            try
            {
                //Directory.CreateDirectory(GetSnapDir(outPath, snap));
                Directory.CreateDirectory(outPath);
            }
            //SUMMARY TODO
            catch (IOException e)
            {
                System.Console.WriteLine(e.Message);
            }

            int curFile = firstSnapshotFile; // index of current read/write file
            int numFile = 0;

            if (lastSnapshotFile < firstSnapshotFile)
            {
                    lastSnapshotFile = firstSnapshotFile;
                    numFile = -1;
            }
            while (curFile <= lastSnapshotFile)
            {
                //DebugOut.PrintLine("..file " + curFile + "/" + lastSnapshotFile);
                Console.WriteLine("..file " + curFile + "/" + lastSnapshotFile);
                SnapFileSummary currentFileSummary = new SnapFileSummary();
                results.Add(currentFileSummary);
                currentFileSummary.start = DateTime.Now;
                
                string filename = "";
                try
                {
                    filename = GetSnapFile(inPath, snap, snapshotFilePrefix, curFile);
                    currentFileSummary.inFileName = filename;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    currentFileSummary.badStatus = true;
                    currentFileSummary.statusMessage = e.Message;
                    return;
                }

                // now load the file
                SnapFile curSnap = new SnapFile(filename);
                // and open the stream for writing
                using (BinaryWriter binwriter = new BinaryWriter(new FileStream(GetSnapDefault(outPath, snap, snapshotFilePrefix, curFile), FileMode.Create)))
                {
                    currentFileSummary.outFileName = GetSnapDefault(outPath, snap, snapshotFilePrefix, curFile);
                    
                    Structs[] parts = new Structs[curSnap.numSample];
                    // now write each particle into the array
                    for (int i = 0; i < curSnap.numSample; i++)
                    {
                        parts[i].x = curSnap.pos[i, 0];
                        parts[i].y = curSnap.pos[i, 1];
                        parts[i].z = curSnap.pos[i, 2];
                        parts[i].vx = curSnap.vel[i, 0];
                        parts[i].vy = curSnap.vel[i, 1];
                        parts[i].vz = curSnap.vel[i, 2];
                        parts[i].snapnum = snap;
                        parts[i].id = curSnap.id[i];
                        // add in highest-order bit
                        parts[i].id |= ((UInt64)curSnap.nLargeSims[1]) << 32;
                        // make ph-key
                        parts[i].phkey = GetPHKey(parts[i].x, parts[i].y, parts[i].z);
                        // read hsml, if desired

                    }
                    // now sort before writing files
                    Array.Sort<Structs>(parts, new ParticleComparator());
                    // and then write output
                    for (int i = 0; i < curSnap.numSample; i++)
                        parts[i].WriteBinaryNoHsml(binwriter);

                    // and add a bulk insert
                    //TODO sql commands file
                    DebugOut.AddCommand(GetSnapDefault(outPath, snap, snapshotFilePrefix, curFile), snapshotTable);

                    DebugOut.PrintLine("..wrote " + curSnap.numSample + "/" + curSnap.numTotals[1] + " points");
                }
                

                // and set numFiles
                if (numFile == -1)
                {
                    numFile = (int)curSnap.numSubfiles - firstSnapshotFile + 1;
                    lastSnapshotFile = numFile - 1;
                }

                currentFileSummary.end = DateTime.Now;
                currentFileSummary.duration = results[curFile].start - results[curFile].end;
                currentFileSummary.numParticles = curSnap.numSample;

                curFile++;
                // avoid outofmemory errors
                GC.Collect();
                GC.WaitForPendingFinalizers();

            }

            globals.summary.setFileSummaries(results);
           
        }
        

    }

    //START NEW CODE

    class IndraFFTDataProcess : Process
    {

        public string inPath = "";
        public string outPath = "";
        public string filePrefix = "";      //"FFT_128_";
        public string fileExtension = "";   //".dat";
        public Int16 snapnumber = -1;
        public Int16 firstSnap = 0;
        public Int16 lastSnap = 0;



        public IndraFFTDataProcess(GlobalParameters globals) : base(globals) { }

        public override void Run()
        {
            File.Delete("C:\\Users\\crankshaw\\Documents\\fft_info.txt");

            if (snapnumber >= 0)
            {
                if (isProcessing)
                        ProcessIndraFFTData(snapnumber);
                else
                {
                    DebugOut.PrintLine("Processing SimDB FFT Data interrupted at snapshot " + snapnumber);
                }
            }
            else
            {
                for (Int16 i = firstSnap; i <= lastSnap; i++)
                {
                    if (isProcessing)
                        ProcessIndraFFTData(i);
                    else
                    {
                        DebugOut.PrintLine("Processing SimDB FFT Data interrupted at snapshot " + i);
                    }
                }
            }
            
        }

        public void ProcessIndraFFTData(int isnap)
        {
            DebugOut.PrintLine("PROCESSING FOF Groups from Snapshot " + isnap);
            int L = 128; //TODO eventually, magic number, assume that there will always
                         // be 128 modes?
            int lhalf = L / 2;

            float[, ,] fft_re = new float[lhalf + +1, L + 1, L + 1];
            float[, ,] fft_im = new float[lhalf + +1, L + 1, L + 1];

            //float[, ,] fft_re = new float[L + 1, L + 1, lhalf + +1];
            //float[, ,] fft_im = new float[L + 1, L + 1, lhalf + +1];

            int nsize = 0;
            double time2 = 0.0;
            string fftfile = GetIndraFFTFILE(inPath, isnap, filePrefix, fileExtension);
            DebugOut.PrintLine(fftfile);

            using (BinaryReader reader = new BinaryReader(new FileStream(fftfile, FileMode.Open, FileAccess.Read)))
            {
                time2 = reader.ReadDouble();
                //nsize = reader.ReadSingle();
                nsize = reader.ReadInt32();

                for (int y = 0; y <= L; y++)
                {
                    for (int x = 0; x <= L; x++)
                    {
                        for (int z = 0; z <= lhalf; z++)
                        {
                            fft_re[z, x, y] = reader.ReadSingle();

                        }
                    }
                }



                for (int y = 0; y <= L; y++)
                {
                    for (int x = 0; x <= L; x++)
                    {
                        for (int z = 0; z <= lhalf; z++)
                        {
                            fft_im[z, x, y] = reader.ReadSingle();
                        }
                    }
                }
            }

            string outfilepath = outPath + "\\" + "trans_" + filePrefix + isnap;

            using (SqlBinaryWriter binwriter = new SqlBinaryWriter(
                new FileStream(outfilepath, FileMode.Create)))
            {
                binwriter.WriteFFTModes((short)isnap, time2, nsize, fft_re, fft_im);
            }
            GC.Collect();
            GC.WaitForPendingFinalizers();
            DebugOut.PrintLine("Completed writing snapshot: " + isnap);
        }

    }






    class IndraFOFProcess : Process
    {

        public string inPath = "";
        public string outPath = "";
        public string groupIDFilePrefix = "group_ids_";
        public string groupTabFilePrefix = "group_tab_";
        public int firstSnapshotFile = 0;
        public int lastSnapshotFile = -1;

        public Int16 snapnumber = -1;
        public Int16 firstSnap = 0;
        public Int16 lastSnap = 0;






        public IndraFOFProcess(GlobalParameters globals)
            : base(globals)
        { }
        public override void Run()
        {

            if (snapnumber >= 0)
            {
                if (isProcessing)
                    ProcessIndraFofs(snapnumber);
                else
                {
                    DebugOut.PrintLine("Processing SimDB FOF groups interrupted at snapshot " + snapnumber);
                }
            }
            else
            {
                for (Int16 i = firstSnap; i <= lastSnap; i++)
                {
                    if (isProcessing)
                        ProcessIndraFofs(i);
                    else
                    {
                        DebugOut.PrintLine("Processing SimDB FOF groups interrupted at snapshot " + snapnumber);
                    }
                }
            }

        }

        public void ProcessIndraFofs(int isnap)
        {
            DebugOut.PrintLine("PROCESSING FOF Groups from Snapshot " + isnap);

            int skip = 0;
            //long skip_sub = 0;
            int filenumber = 0;
            string groupTabFile = "";
            string groupIDFile = "";
            int[] GroupLen = new int[0];
            int[] GroupOffset = new int[0];
            long[] IDs = new long[0];
            bool done = false;
            int Ntask = 0;
            int nGroups = 0;
            int totNGroups = 0;
            int NIds = 0;
            FOFSummary summary = new FOFSummary();
            summary.start = DateTime.Now;
            while (!done)
            {
                groupTabFile = GetSimDBFOFFile(this.inPath, isnap, this.groupTabFilePrefix, filenumber);


                using (BinaryReader reader = new BinaryReader(new FileStream(groupTabFile, FileMode.Open, FileAccess.Read)))
                {

                    nGroups = reader.ReadInt32();
                    NIds = reader.ReadInt32();
                    totNGroups = reader.ReadInt32();
                    Ntask = reader.ReadInt32();
                    if (filenumber == 0)
                    {
                        GroupLen = new int[totNGroups];
                        GroupOffset = new int[totNGroups];
                    }
                    if (nGroups > 0)
                    {
                        for (int i = skip; i < skip + nGroups; i++)
                        {
                            GroupLen[i] = reader.ReadInt32();
                        }
                        for (int i = skip; i < skip + nGroups; i++)
                        {
                            GroupOffset[i] = reader.ReadInt32();
                        }
                        skip += nGroups;
                    }
                }
                DebugOut.PrintLine("Ngroups: " + nGroups + " totNGroups: " + totNGroups + "  NIds: " + NIds + "  Ntask: " + Ntask);
                filenumber++;
                //if (filenumber > lastSnapshotFile)
                // {
                //   done = true;
                //}
                if (filenumber == Ntask)
                {
                    done = true;
                }

            }



            nGroups = 0;
            totNGroups = 0;
            NIds = 0;
            Ntask = 0;
            done = false;
            filenumber = 0;
            skip = 0;

            while (!done)
            {
                groupIDFile = GetSimDBFOFFile(this.inPath, isnap, this.groupIDFilePrefix, filenumber);


                using (BinaryReader reader = new BinaryReader(new FileStream(groupIDFile, FileMode.Open, FileAccess.Read)))
                {



                    nGroups = reader.ReadInt32();
                    NIds = reader.ReadInt32();
                    totNGroups = reader.ReadInt32();
                    Ntask = reader.ReadInt32();
                    int totNIds = 0;
                    for (int i = 0; i < GroupLen.Length; i++)
                    {
                        totNIds += GroupLen[i];
                    }

                    if (filenumber == 0)
                    {
                        IDs = new long[totNIds];


                    }
                    if (NIds > 0)
                    {
                        for (int i = skip; i < skip + NIds; i++)
                        {
                            IDs[i] = reader.ReadInt64();
                            IDs[i] &= (( ((long)1) << 34) - 1);
                        }

                        skip += NIds;
                    }
                }
                //DebugOut.PrintLine("Ngroups: " + nGroups + " totNGroups: " + totNGroups + "  NIds: " + NIds + "  Ntask: " + Ntask);
                filenumber++;
                //if (filenumber > lastSnapshotFile)
                // {
                //   done = true;
                // }
                if (filenumber == Ntask)
                {
                    done = true;
                }
            }


            string outfilepath = this.outPath + "//groups_snap" + isnap;

            using (SqlBinaryWriter binwriter = new SqlBinaryWriter(
                new FileStream(outfilepath, FileMode.Create)))
            {
                for (int i = 0; i < totNGroups; i++)
                {

                    if (GroupLen[i] > 0)
                    {
                        long[] curGroupIds = new long[GroupLen[i]];
                        Array.Copy(IDs, GroupOffset[i], curGroupIds, 0, curGroupIds.Length);
                        // Now add the ID to a bloom filter
                        BloomFilter.Filter<long> filter = new Filter<long>(LoaderConstants.expectedSize);
                        for (int j = 0; j < curGroupIds.Length; j++)
                        {
                            filter.Add(curGroupIds[j]);
                        }

                        binwriter.WriteFoFGroup(curGroupIds, (short)isnap, i, filter);
                    }
                }
            }
            summary.end = DateTime.Now;
            summary.duration = summary.end - summary.start;
            summary.numGroups = totNGroups;
            globals.summary.fofSummary = summary;

            GC.Collect();
            GC.WaitForPendingFinalizers();
            DebugOut.PrintLine("Completed writing snapshot: " + isnap);
        }

    }



    //END NEW CODE


 



    class GlobalParameters
    {
        // redshifts
        public List<float> redshifts;

        public int phbits = 8;
        public int numzones = 20;
        public double box = 100;
        public double phboxinv;
        public double zoneinv;
        public int maxRandom;
        public Random random;
        public LoaderSummary summary;


        /*public GlobalParameters(int _phbits, int _numZones, float _box, int _maxRandom
          , string redshiftFile, string sqlCommandsFile)*/

        public GlobalParameters(
            int _phbits, int _numZones, float _box, int _maxRandom
            , string sqlCommansdsFile, string server, string database)
        {
            phbits = _phbits;
            numzones = _numZones;
            box = _box;
            phboxinv = ((double)(1 << phbits)) / box;
            zoneinv = ((double)numzones) / box;

            maxRandom = _maxRandom;
            random = new Random();
            summary = new LoaderSummary(server, database);
            summary.setBCPFile(sqlCommansdsFile);
            
            /*initRedshifts(redshiftFile);*/

        }

        public GlobalParameters(
            int _phbits, int _numZones, float _box, int _maxRandom
            , string sqlCommansdsFile)
        {
            phbits = _phbits;
            numzones = _numZones;
            box = _box;
            phboxinv = ((double)(1 << phbits)) / box;
            zoneinv = ((double)numzones) / box;

            maxRandom = _maxRandom;
            random = new Random();
            summary = new LoaderSummary(LoaderConstants.server, LoaderConstants.database);
            summary.setBCPFile(sqlCommansdsFile);

            /*initRedshifts(redshiftFile);*/

        }

        /// <summary>
        /// Read redshifts as function of snapnum form file.
        /// </summary>
        /// <param name="file"></param>
        private void initRedshifts(string file)
        {
            redshifts = new List<float>();
            using (System.IO.StreamReader sr = System.IO.File.OpenText(file))
            {
                string line = "";
                while ((line = sr.ReadLine()) != null)
                {
                    if (!line.StartsWith("#"))
                    {
                        string[] words = Regex.Split(line, "[\t ]+");
                        if (words.Length > 1)
                        {
                            float z = float.Parse(words[2].Trim());
                            redshifts.Add(z);
                        }
                    }
                }
            }
        }




    }
    abstract class Process
    {
        public GlobalParameters globals;
        public Runner runner;
        // Runtime parameters
        public bool isProcessing
        {
            get { return runner.isProcessing; }
        }
        // Parameters
        private String InPath;
        private String OutPath;

        public string inPath
        {
            get { return InPath; }
            set
            {
                InPath = value.TrimEnd('\\');
            }
        }

        public string outPath
        {
            get { return OutPath; }
            set
            {
                OutPath = value.TrimEnd('\\');
            }
        }

        public Process(GlobalParameters _globals)
        {
            globals = _globals;
        }
        public abstract void Run();

        public long CalculateMainLeafId(TreeInfo current, TreeInfo root, Trees trees)
        {
            if (current.LastProgenitorId == current.Id)
                current.MainLeafId = current.Id;
            else
            {
                TreeInfo next = trees.get(current.Id - root.Id + 1);
                current.MainLeafId = CalculateMainLeafId(next, root, trees);

                while (next.LastProgenitorId < current.LastProgenitorId)
                {
                    next = trees.get(next.LastProgenitorId - root.Id + 1);
                    CalculateMainLeafId(next, root, trees);
                }
            }
            return current.MainLeafId;
        }



        public long MakeSubhaloFileID(int snap, int file, int index)
        {
            return (snap * 10000L + file) * 1000000L + index;
        }
        public long MakeGroupID(int snap, int file, int index)
        {
            if (index > 1000000L)
                Console.Write("index too large!");
            return (snap * 10000L + file) * 1000000L + index;
        }
        public long MakeSubhaloFOFID(long fofId, int rank)
        {
            return (fofId * 1000000 + rank);
        }

        public int GetPHKey(double x, double y, double z)
        {
            int ix = (int)Math.Floor(x * globals.phboxinv);
            int iy = (int)Math.Floor(y * globals.phboxinv);
            int iz = (int)Math.Floor(z * globals.phboxinv);
            return PeanoHilbertID.GetPeanoHilbertID(globals.phbits, ix, iy, iz);
        }

        protected String FormatSnap(int snap)
        {
            if (snap < 10)
                return "00" + Convert.ToString(snap);
            if (snap < 100)
                return "0" + Convert.ToString(snap);
            return Convert.ToString(snap);
        }

        protected string GetSnapDir(string path, int snap)
        {
            return path + "\\snapdir_" + FormatSnap(snap);
        }

        protected string GetGroupDir(string path, int snap)
        {
            return path + "\\groups_" + FormatSnap(snap);
        }

        protected string GetGalaxiesDir(string path)
        {
            return path + "\\galaxies\\";
        }
        protected string GetGalaxiesOutFile(string path, int vol)
        {
            return GetGalaxiesDir(path) + "\\galaxies." + vol;
        }
        protected string GetGroupTab(string path, int snap, int file)
        {
            return GetGroupDir(path, snap) + "\\group_tab_" + FormatSnap(snap)
                + "." + file;
        }
        public string GetSubTab(string path, int snap, int file)
        {
            return GetGroupDir(path, snap) + "\\subhalo_tab_" + FormatSnap(snap)
                + "." + file;
        }
        protected string GetSubhaloFile(string path, int snap)
        {
            return GetGroupDir(path, snap) + "\\subhalos_" + FormatSnap(snap);
        }

        protected string GetGroupFile(string path, int snap)
        {
            return GetGroupDir(path, snap) + "\\fof_" + FormatSnap(snap);
        }

        protected string GetSubID(string path, int snap, int file)
        {
            return GetGroupDir(path, snap) + "\\subhalo_ids_" + FormatSnap(snap)
                + "." + file;
        }

        protected string GetSnapDefault(string path, int snap, string filePrefix, int file)
        {
            String s_snap = FormatSnap(snap);
            return path + "\\" + filePrefix + s_snap + "." + file;
        }

        protected string GetSnapIdDefault(string path, int snap, string filePrefix, int file)
        {
            String s_snap = FormatSnap(snap);
            return path + "\\snapdir_" + s_snap + "\\" + filePrefix + "id_" + s_snap + "." + file;
        }
        protected string GetSnapPosDefault(string path, int snap, string filePrefix, int file)
        {
            String s_snap = FormatSnap(snap);
            return path + "\\snapdir_" + s_snap + "\\" + filePrefix + "pos_" + s_snap + "." + file;
        }
        protected string GetSnapVelDefault(string path, int snap, string filePrefix, int file)
        {
            String s_snap = FormatSnap(snap);
            return path + "\\snapdir_" + s_snap + "\\" + filePrefix + "vel_" + s_snap + "." + file;
        }

        protected string GetSnapFile(string path, int snap, string prefix, int file)
        {
            DirectoryInfo dir = new DirectoryInfo(path);
            // find file matching query, regardless of name
            FileInfo[] files;
            if (snap < 10)
                files = dir.GetFiles(prefix + "00" + snap + "." + file);
            else
                files = dir.GetFiles(prefix + "0" + snap + "." + file);
            if (files.Length > 1)
                throw new Exception("Too many files matching search in " + path);
            if (files.Length == 0)
                throw new Exception("No files matching search in " + path);
            return files[0].FullName;
        }
        protected string GetSimDBFOFFile(string path, int snap, string prefix, int file)
        {
            DirectoryInfo dir = new DirectoryInfo(path);
            // find file matching query, regardless of name
            FileInfo[] files;
            if (snap < 10)
            {
                files = dir.GetFiles(prefix + "00" + snap + "." + file);
            }
            else if (snap < 100)
            {
                files = dir.GetFiles(prefix + "0" + snap + "." + file);
            }
            else
                files = dir.GetFiles(prefix + snap + "." + file);
            if (files.Length > 1)
                throw new Exception("Too many files matching search in " + GetSnapDir(path, snap));
            if (files.Length == 0)
                throw new Exception("No files matching search in " + GetSnapDir(path, snap));
            return files[0].FullName;
        }

        protected string GetIndraFFTFILE(string path, int snap, string prefix, string ext)
        {
            DirectoryInfo dir = new DirectoryInfo(path);
            FileInfo[] files;
            if (snap < 10)
            {
                files = dir.GetFiles(prefix + "00" + snap + ext);
            }
            else if (snap < 100)
            {
                files = dir.GetFiles(prefix + "0" + snap + ext);
            }
            else
            {
                files = dir.GetFiles(prefix + snap + ext);
            }
            if (files.Length > 1)
                throw new Exception("Too many files matching search in " + GetSnapDir(path, snap));
            if (files.Length == 0)
                throw new Exception("No files matching search in " + GetSnapDir(path, snap));
            return files[0].FullName;
        }



        protected string GetHsmlPath(string path, int snap)
        {
            return path + "\\hsmldir_" + FormatSnap(snap) + "\\hsml_" + FormatSnap(snap);
        }

        public int GetZone(float x)
        {
            return (int)Math.Floor(x * globals.zoneinv);
        }

    }
}
