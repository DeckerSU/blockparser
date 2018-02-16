
#include <util.h>
#include <timer.h>
#include <common.h>
#include <errlog.h>
#include <callback.h>

#include <string>
#include <vector>
#include <fcntl.h>
#include <malloc.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#if _MSC_VER
#define lseek(fp, offset, whence) _lseeki64(fp, offset, whence)
#endif // _MS


#if !defined(S_ISDIR)
    #define S_ISDIR(mode) (S_IFDIR==((mode) & S_IFMT))
#endif

typedef GoogMap<
    Hash256,
    Chunk*,
    Hash256Hasher,
    Hash256Equal
>::Map TXOMap;

typedef GoogMap<
    Hash256,
    Block*,
    Hash256Hasher,
    Hash256Equal
>::Map BlockMap;

static bool gNeedUpstream;
static Callback *gCallback;

static const BlockFile *gCurBlockFile;
static std::vector<BlockFile> blockFiles;

static TXOMap gTXOMap;
static BlockMap gBlockMap;
static uint8_t empty[kSHA256ByteSize] = { 0x42 };

static Block *gMaxBlock;
static Block *gNullBlock;
static int64_t gMaxHeight;
static uint64_t gChainSize;
static uint256_t gNullHash;

static double getMem() {

    #if defined(linux)
        char statFileName[256];
        sprintf(
            statFileName,
            "/proc/%d/statm",
            (int)getpid()
        );

        uint64_t mem = 0;
        FILE *f = fopen(statFileName, "r");
            if(1!=fscanf(f, "%" PRIu64, &mem)) {
                warning("coudln't read process size");
            }
        fclose(f);
        return (1e-9f*mem)*getpagesize();
    #elif defined(_WIN64)
        return 0;   // TODO
    #else
        return 0;   // TODO
    #endif
}

/*

gExpectedMagic - from chainparams.cpp:

        pchMessageStart[0] = 0xf9; 
        pchMessageStart[1] = 0xbe;
        pchMessageStart[2] = 0xb4;
        pchMessageStart[3] = 0xd9;
*/

#if defined BITCOIN
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".bitcoin";
    static const uint32_t gExpectedMagic = 0xd9b4bef9;
#endif

#if defined TESTNET3
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".bitcoin/testnet3";
    static const uint32_t gExpectedMagic = 0x0709110b;
#endif

#if defined LITECOIN
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".litecoin";
    static const uint32_t gExpectedMagic = 0xdbb6c0fb;
#endif

#if defined DARKCOIN
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".darkcoin";
    static const uint32_t gExpectedMagic = 0xbd6b0cbf;
#endif

#if defined PROTOSHARES
    static const size_t gHeaderSize = 88;
    static auto kCoinDirName = ".protoshares";
    static const uint32_t gExpectedMagic = 0xd9b5bdf9;
#endif

#if defined FEDORACOIN
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".fedoracoin";
    static const uint32_t gExpectedMagic = 0xdead1337;
#endif

#if defined PEERCOIN
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".ppcoin";
    static const uint32_t gExpectedMagic = 0xe5e9e8e6;
#endif

#if defined CLAM
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".clam";
    static const uint32_t gExpectedMagic = 0x15352203;
#endif

#if defined PAYCON
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".PayCon";
    static const uint32_t gExpectedMagic = 0x2d3b3c4b;
#endif

#if defined JUMBUCKS
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".coinmarketscoin";
    static const uint32_t gExpectedMagic = 0xb6f1f4fc;
#endif

#if defined MYRIADCOIN
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".myriadcoin";
    static const uint32_t gExpectedMagic = 0xee7645af;
#endif

#if defined UNOBTANIUM
    static const size_t gHeaderSize = 80;
    static auto kCoinDirName = ".unobtanium";
    static const uint32_t gExpectedMagic = 0x03b5d503;
#endif

#if defined DTT
    static const size_t gHeaderSize = 140; // 4+32+32+32+4+4+32; // excluding Equihash solution
#ifndef _WIN32
	static auto kCoinDirName = ".komodo/DTT";
#else
	static auto kCoinDirName = "Komodo/DTT";
#endif // !_WIN32
    static const uint32_t gExpectedMagic = 0xbb0700df;
#endif

#if defined KOMODO
    static const size_t gHeaderSize = 140; // 4+32+32+32+4+4+32; // excluding Equihash solution
#ifndef _WIN32
	static auto kCoinDirName = ".komodo";
#else
	static auto kCoinDirName = "Komodo";
#endif // !_WIN32

	
    static const uint32_t gExpectedMagic = 0x8de4eef9;
#endif


#define DO(x) x
    static inline void   startBlock(const uint8_t *p)                      { DO(gCallback->startBlock(p));         }
    static inline void     endBlock(const uint8_t *p)                      { DO(gCallback->endBlock(p));           }
    static inline void     startTXs(const uint8_t *p)                      { DO(gCallback->startTXs(p));           }
    static inline void       endTXs(const uint8_t *p)                      { DO(gCallback->endTXs(p));             }
    static inline void      startTX(const uint8_t *p, const uint8_t *hash) { DO(gCallback->startTX(p, hash));      }
    static inline void        endTX(const uint8_t *p)                      { DO(gCallback->endTX(p));              }
    static inline void  startInputs(const uint8_t *p)                      { DO(gCallback->startInputs(p));        }
    static inline void    endInputs(const uint8_t *p)                      { DO(gCallback->endInputs(p));          }
    static inline void   startInput(const uint8_t *p)                      { DO(gCallback->startInput(p));         }
    static inline void     endInput(const uint8_t *p)                      { DO(gCallback->endInput(p));           }
    static inline void startOutputs(const uint8_t *p)                      { DO(gCallback->startOutputs(p));       }
    static inline void   endOutputs(const uint8_t *p)                      { DO(gCallback->endOutputs(p));         }
    static inline void  startOutput(const uint8_t *p)                      { DO(gCallback->startOutput(p));        }
    static inline void        start(const Block *s, const Block *e)        { DO(gCallback->start(s, e));           }
#undef DO

static inline void   startBlockFile(const uint8_t *p)                      { gCallback->startBlockFile(p);         }
static inline void     endBlockFile(const uint8_t *p)                      { gCallback->endBlockFile(p);           }
static inline void         startBlock(const Block *b)                      { gCallback->startBlock(b, gChainSize); }
static inline void           endBlock(const Block *b)                      { gCallback->endBlock(b);               }
static inline bool                             done()                      { return gCallback->done();             }

static inline void endOutput(
    const uint8_t *p,
    uint64_t      value,
    const uint8_t *txHash,
    uint64_t      outputIndex,
    const uint8_t *outputScript,
    uint64_t      outputScriptSize
) {
    gCallback->endOutput(
        p,
        value,
        txHash,
        outputIndex,
        outputScript,
        outputScriptSize
    );
}

static inline void edge(
    uint64_t      value,
    const uint8_t *upTXHash,
    uint64_t      outputIndex,
    const uint8_t *outputScript,
    uint64_t      outputScriptSize,
    const uint8_t *downTXHash,
    uint64_t      inputIndex,
    const uint8_t *inputScript,
    uint64_t      inputScriptSize
) {
    gCallback->edge(
        value,
        upTXHash,
        outputIndex,
        outputScript,
        outputScriptSize,
        downTXHash,
        inputIndex,
        inputScript,
        inputScriptSize
    );
}

template<
    bool skip,
    bool fullContext
>
static void parseOutput(
    const uint8_t *&p,
    const uint8_t *txHash,
    uint64_t      outputIndex,
    const uint8_t *downTXHash,
    uint64_t      downInputIndex,
    const uint8_t *downInputScript,
    uint64_t      downInputScriptSize,
    bool          found = false
) {
    if(!skip && !fullContext) {
        startOutput(p);
    }

        LOAD(uint64_t, value, p);
        LOAD_VARINT(outputScriptSize, p);

        auto outputScript = p;
        p += outputScriptSize;

        if(!skip && fullContext && found) {
            edge(
                value,
                txHash,
                outputIndex,
                outputScript,
                outputScriptSize,
                downTXHash,
                downInputIndex,
                downInputScript,
                downInputScriptSize
            );
        }

    if(!skip && !fullContext) {
        endOutput(
            p,
            value,
            txHash,
            outputIndex,
            outputScript,
            outputScriptSize
        );
    }
}

template<
    bool skip,
    bool fullContext
>
static void parseOutputs(
    const uint8_t *&p,
    const uint8_t *txHash,
    uint64_t      stopAtIndex = -1,
    const uint8_t *downTXHash = 0,
    uint64_t      downInputIndex = 0,
    const uint8_t *downInputScript = 0,
    uint64_t      downInputScriptSize = 0
) {
    if(!skip && !fullContext) {
        startOutputs(p);
    }

        LOAD_VARINT(nbOutputs, p);
        for(uint64_t outputIndex=0; outputIndex<nbOutputs; ++outputIndex) {
            auto found = (fullContext && !skip && (stopAtIndex==outputIndex));
            parseOutput<skip, fullContext>(
                p,
                txHash,
                outputIndex,
                downTXHash,
                downInputIndex,
                downInputScript,
                downInputScriptSize,
                found
            );
            if(found) {
                break;
            }
        }

    if(!skip && !fullContext) {
        endOutputs(p);
    }
}

template<
    bool skip
>
static void parseInput(
    const Block   *block,
    const uint8_t *&p,
    const uint8_t *txHash,
    uint64_t      inputIndex
) {
    if(!skip) {
        startInput(p);
    }

        auto upTXHash = p;
        const Chunk *upTX = 0;
        if(gNeedUpstream && !skip) {
            auto isGenTX = (0==memcmp(gNullHash.v, upTXHash, sizeof(gNullHash)));
            if(likely(false==isGenTX)) {
                auto i = gTXOMap.find(upTXHash);
                if(unlikely(gTXOMap.end()==i)) {
                    printf("txhash: "); for (int dck_index = 31; dck_index >= 0; dck_index--) printf("%02x",txHash[dck_index]); printf("\n");	
                    errFatal("failed to locate upstream transaction");
                }
                upTX = i->second;
            }
        }

        SKIP(uint256_t, dummyUpTXhash, p); // tx hash, each input refers to an output in previous tx (for coinbase hash = 0)
        LOAD(uint32_t, upOutputIndex, p); // index refers to an output in previous tx (for coinbase = 0xFFFFFFFF)
        LOAD_VARINT(inputScriptSize, p); // variable length integer : scriptLength : The length of script byte data following 

		// if upTX == 0 (in coinbase tx case too, skip parsing upTX)
        if(!skip && 0!=upTX) {
            auto inputScript = p;
            auto upTXOutputs = upTX->getData();
                parseOutputs<false, true>(
                    upTXOutputs,
                    upTXHash,
                    upOutputIndex,
                    txHash,
                    inputIndex,
                    inputScript,
                    inputScriptSize
                );
            upTX->releaseData();
        }

        p += inputScriptSize;
        SKIP(uint32_t, sequence, p);

    if(!skip) {
        endInput(p);
    }
}

template<
    bool skip
>
static void parseInputs(
    const Block   *block,
    const uint8_t *&p,
    const uint8_t *txHash
) {
    if(!skip) {
        startInputs(p);
    }

    LOAD_VARINT(nbInputs, p);
    for(uint64_t inputIndex=0; inputIndex<nbInputs; ++inputIndex) {
        parseInput<skip>(
            block,
            p,
            txHash,
            inputIndex
        );
    }

    if(!skip) {
        endInputs(p);
    }
}

template<
    bool skip
>
static void parseTX(
    const Block   *block,
    const uint8_t *&p
) {
    auto txStart = p;
    uint8_t *txHash = 0;

    if(gNeedUpstream && !skip) {
		printf("[Decker] if(gNeedUpstream && !skip) { ... }\n");
		auto txEnd = p;
        txHash = allocHash256();
        parseTX<true>(block, txEnd);
        sha256Twice(txHash, txStart, txEnd - txStart);
    }

    if(!skip) {
        startTX(p, txHash);
    }

        #if defined(CLAM) || defined(KOMODO) || defined(DTT)
            LOAD(uint32_t, nVersion, p);
        #else
            SKIP(uint32_t, nVersion, p);
        #endif

        #if defined(PEERCOIN) || defined(CLAM) || defined(JUMBUCKS) || defined(PAYCON)
            SKIP(uint32_t, nTime, p);
        #endif

        parseInputs<skip>(block, p, txHash);

        Chunk *txo = 0;
        size_t txoOffset = -1;
        const uint8_t *outputsStart = p;
        if(gNeedUpstream && !skip) {
            txo = Chunk::alloc();
            gTXOMap[txHash] = txo;
            txoOffset = block->chunk->getOffset() + (p - block->chunk->getData());
        }

		parseOutputs<skip, false>(p, txHash);
		const uint8_t *outputsEnd = p;

        if(txo) {
            size_t txoSize = p - outputsStart;
            txo->init(
                block->chunk->getBlockFile(),
                txoSize,
                txoOffset
            );
        }

        SKIP(uint32_t, lockTime, p);

        #if defined(CLAM)
            if(1<nVersion) {
                LOAD_VARINT(strCLAMSpeechLen, p);
                p += strCLAMSpeechLen;
            }
        #endif

		#if defined(KOMODO) || defined(DTT)	
			if (1 < nVersion) {
				// for KMD and assets we should somehow handle nVersion, if nVersion == 1 - this is common transaction,
				// but if nVersion == 2 - seems it JoinSplit tx and we should somehow handle this situation. 

				
				printf("[Decker] Block #%8" PRIu64 "\n", block->height);

				uint8_t JSDescriptionIndex;
				LOAD(uint8_t, JSDescriptionCount, p);

				for (JSDescriptionIndex = 0; JSDescriptionIndex < JSDescriptionCount; JSDescriptionIndex++) {

					LOAD(uint64_t, vpub_old, p);
					LOAD(uint64_t, vpub_new, p);

					//printf("[Decker] skip = 0x%08x\n", skip);
					printf("[Decker] JSDescriptionCount = %d, vpub_old = %" PRIu64 ", vpub_new = %" PRIu64 "\n", JSDescriptionCount, vpub_old, vpub_new);
					
					p += 1786;
				}

				p += 96;

				// before exit from this proc we should move pointer p to the end of current tx [!]


				/*if (JSDescriptionCount == 2) {
					printf("[Decker] p = 0x%" PRIx64 ", block->chunk->getData() = 0x%" PRIx64 "\n", p, block->chunk->getData());
				}*/
			}
		#endif


    if(!skip) {
        endTX(p);
    }
}

static bool parseBlock(
    const Block *block
) {
    startBlock(block);
        auto p = block->chunk->getData();

            auto header = p;
	    #if defined(KOMODO) || defined(DTT)

 	    // int i;	

            SKIP(uint32_t, version, p);
            SKIP(uint256_t, prevBlkHash, p);
            SKIP(uint256_t, blkMerkleRoot, p);
            SKIP(uint256_t, blkhashReserved, p);
            SKIP(uint32_t, blkTime, p);

            SKIP(uint32_t, blkBits, p);
            SKIP(uint256_t, blkNonce, p);
            LOAD_VARINT(nSolutionSize, p);
        
	    /*
	    for (i=0; i<nSolutionSize; i++) printf("%02x", (unsigned char)p[i]);
	    printf("\n---\n");
	    */

            p += nSolutionSize;

	    /*
	    for (i=0; i<1024; i++) printf("%02x", (unsigned char)p[i]);
	    printf("\n---\n");
	    */

	    #else
            SKIP(uint32_t, version, p);
            SKIP(uint256_t, prevBlkHash, p);
            SKIP(uint256_t, blkMerkleRoot, p);
            SKIP(uint32_t, blkTime, p);
            SKIP(uint32_t, blkBits, p);
            SKIP(uint32_t, blkNonce, p);
	    #endif

            #if defined PROTOSHARES
                SKIP(uint32_t, nBirthdayA, p);
                SKIP(uint32_t, nBirthdayB, p);
            #endif

            startTXs(p);
                LOAD_VARINT(nbTX, p);
                for(uint64_t txIndex=0; likely(txIndex<nbTX); ++txIndex) {
                    parseTX<false>(block, p);
                    if(done()) {
                        return true;
                    }
                }
            endTXs(p);

            #if defined(PEERCOIN) || defined(CLAM) || defined(JUMBUCKS) || defined(PAYCON)
                LOAD_VARINT(vchBlockSigSize, p);
                p += vchBlockSigSize;
            #endif

        block->chunk->releaseData();
    endBlock(block);
    return done();
}

static void parseLongestChain() {

    info(
        "pass 4 -- full blockchain analysis (with%s index)...",
        gNeedUpstream ? "" : "out"
    );

    auto startTime = Timer::usecs();
    gCallback->startLC();

        uint64_t bytesSoFar =  0;
        auto blk = gNullBlock->next;
        start(blk, gMaxBlock);

        while(likely(0!=blk)) {

            if(0==(blk->height % 10)) {
   
                auto now = Timer::usecs();
                static auto last = -1.0;
                auto elapsedSinceLastTime = now - last;
                auto elapsedSinceStart = now - startTime;
                auto progress =  bytesSoFar/(double)gChainSize;
                auto bytesPerSec = bytesSoFar / (elapsedSinceStart*1e-6);
                auto bytesLeft = gChainSize - bytesSoFar;
                auto secsLeft = bytesLeft / bytesPerSec;
                if((1.0 * 1000 * 1000)<elapsedSinceLastTime) {
                    fprintf(
                        stderr,
                        "block %6d/%6d, %.2f%% done, ETA = %.2fsecs, mem = %.3f Gig           \r",
                        (int)blk->height,
                        (int)gMaxHeight,
                        progress*100.0,
                        secsLeft,
                        getMem()
                    );
                    fflush(stderr);
                    last = now;
                }
            }

            if(parseBlock(blk)) {
                break;
            }

            bytesSoFar += blk->chunk->getSize();
            blk = blk->next;
        }

    fprintf(stderr, "                                                          \r");
    gCallback->wrapup();

    info("pass 4 -- done.");
}

static void wireLongestChain() {

    info("pass 3 -- wire longest chain ...");

    auto block = gMaxBlock;
    while(1) {
        auto prev = block->prev;
        if(unlikely(0==prev)) {
            break;
        }
        prev->next = block;
        block = prev;
    }

    info(
        "pass 3 -- done, maxHeight=%d",
        (int)gMaxHeight
    );
}

static void initCallback(
    int   argc,
    char *argv[]
) {
    const char *methodName = 0;
    if(0<argc) {
        methodName = argv[1];
    }
    if(0==methodName) {
        methodName = "";
    }
    if(0==methodName[0]) {
        methodName = "help";
    }
    gCallback = Callback::find(methodName);

    info("starting command \"%s\"", gCallback->name());
    if(argv[1]) {
        auto i = 0;
        while('-'==argv[1][i]) {
            argv[1][i++] = 'x';
        }
    }

    auto ir = gCallback->init(argc, (const char **)argv);
    if(ir<0) {
        errFatal("callback init failed");
    }
    gNeedUpstream = gCallback->needUpstream();

    if(done()) {
        fprintf(stderr, "\n");
        exit(0);
    }
}

static void findBlockParent(
    Block *b
) {
    auto where = lseek64(
        b->chunk->getBlockFile()->fd,
        b->chunk->getOffset(),
        SEEK_SET
    );
    if(where!=(signed)b->chunk->getOffset()) {
        sysErrFatal(
            "failed to seek into block chain file %s",
            b->chunk->getBlockFile()->name.c_str()
        );
    }

    uint8_t buf[gHeaderSize];
    auto nbRead = read(
        b->chunk->getBlockFile()->fd,
        buf,
        gHeaderSize
    );
    if(nbRead<(signed)gHeaderSize) {
        sysErrFatal(
            "failed to read from block chain file %s",
            b->chunk->getBlockFile()->name.c_str()
        );
    }

    auto i = gBlockMap.find(4 + buf);
    if(unlikely(gBlockMap.end()==i)) {

        uint8_t bHash[2*kSHA256ByteSize + 1];
        toHex(bHash, b->hash);

        uint8_t pHash[2*kSHA256ByteSize + 1];
        toHex(pHash, 4 + buf);

        warning(
            "in block %s failed to locate parent block %s",
            bHash,
            pHash
        );
        return;
    }
    b->prev = i->second;
}

static void computeBlockHeight(
    Block  *block,
    size_t &lateLinks
) {

    if(unlikely(gNullBlock==block)) {
        return;
    }

    auto b = block;
    while(b->height<0) {

        if(unlikely(0==b->prev)) {

            findBlockParent(b);
            ++lateLinks;

            if(0==b->prev) {
                warning("failed to locate parent block");
                return;
            }
        }

        b->prev->next = b;
        b = b->prev;
    }

    auto height = b->height;
    while(1) {

        b->height = height++;

        if(likely(gMaxHeight<b->height)) {
            gMaxHeight = b->height;
            gMaxBlock = b;
        }

        auto next = b->next;
        b->next = 0;

        if(block==b) {
            break;
        }
        b = next;
    }
}

static void computeBlockHeights() {

    size_t lateLinks = 0;
    info("pass 2 -- link all blocks ...");
    for(const auto &pair:gBlockMap) {
        computeBlockHeight(pair.second, lateLinks);
    }
    info(
        "pass 2 -- done, did %d late links",
        (int)lateLinks
    ); 
}

static void getBlockHeader(
    size_t        &size,
    Block        *&prev,
    uint8_t      *&hash,
    size_t        &earlyMissCnt,
    const uint8_t *p
) {

    LOAD(uint32_t, magic, p);
    if(unlikely(gExpectedMagic!=magic)) {
        hash = 0;
        return;
    }

    LOAD(uint32_t, sz, p);
    size = sz;
    prev = 0;

    hash = allocHash256();

    #if (defined(KOMODO) || defined(DTT))
        int dck_index;
        const uint8_t *pt, *pt_prev;
        size_t additional_size;

        pt = p;
        SKIP(uint32_t, version, pt);
        SKIP(uint256_t, prevBlkHash, pt);
        SKIP(uint256_t, blkMerkleRoot, pt);
        SKIP(uint256_t, blkhashReserved, pt);
        SKIP(uint32_t, blkTime, pt);
        SKIP(uint32_t, blkBits, pt);
        SKIP(uint256_t, blkNonce, pt);
        pt_prev = pt;
        LOAD_VARINT(blkSolutionsize, pt);
	additional_size = pt - pt_prev;
    
        //printf("[Decker] blkSolutionsize = %d\n",blkSolutionsize);
        //printf("[Decker] buf_header_size = %d\n",size);
        //printf("[Decker] additional_size = %d\n",additional_size);

        //for (dck_index = 0; dck_index < gHeaderSize + blkSolutionsize + additional_size; dck_index++) printf("%02x",p[dck_index]); printf("\n");
        sha256Twice(hash, p, gHeaderSize + blkSolutionsize + additional_size); // hashing all block header, including solution (!)
    #elif defined(DARKCOIN)
        h9(hash, p, gHeaderSize);
    #elif defined(PAYCON)
        h13(hash, p, gHeaderSize);
    #elif defined(CLAM)
        auto pBis = p;
        LOAD(uint32_t, nVersion, pBis);
        if(6<nVersion) {
            sha256Twice(hash, p, gHeaderSize);
        } else {
            scrypt(hash, p, gHeaderSize);
        }
    #elif defined(JUMBUCKS)
        scrypt(hash, p, gHeaderSize);
    #else
        sha256Twice(hash, p, gHeaderSize);
    #endif

    auto i = gBlockMap.find(p + 4);
    if(likely(gBlockMap.end()!=i)) {
        prev = i->second;
    } else {
        ++earlyMissCnt;
    }
}

static void buildBlockHeaders() {

    info("pass 1 -- walk all blocks and build headers ...");

    size_t nbBlocks = 0;
    size_t baseOffset = 0;
    size_t earlyMissCnt = 0;
    uint8_t buf[8+gHeaderSize];
    #if (defined(KOMODO) || defined(DTT))	 
    uint8_t *buf_header;
    uint32_t buf_header_size;
    const uint8_t *p;
    #endif
    const auto sz = sizeof(buf);
    const auto startTime = Timer::usecs();
    const auto oneMeg = 1024 * 1024;

    for(const auto &blockFile : blockFiles) {

        startBlockFile(0);

        while(1) {
			
			uint64_t pos = lseek(blockFile.fd, 0L, SEEK_CUR);
			//printf("[Decker][+] filepos = 0x%" PRIx64 " %s\n", pos, blockFile.name.c_str());
			
			//if (pos == 0x07fff714 || pos == 0x07fff0cf) {
			//	printf("[Decker] Should investigate this ... \n");
			//}

			auto nbRead = read(blockFile.fd, buf, sz);
            if(nbRead<(signed)sz) {
				//printf("\n[Decker][1] reading block #%d\n", nbBlocks);
				break;
            }

		/* Edge cases */

		// edge case, like f9 ee e4 8d d6 06 00 00 f9 ee e4 8d d6 06
		// block header starts twice
			if ((*(uint32_t *)buf == gExpectedMagic) && (*(uint32_t *)(buf + 8) == gExpectedMagic)) {
			printf("[Decker][1] edge case #%d\n", nbBlocks);
			printf("[Decker][-] filepos = 0x%" PRIx64 " %s\n", lseek(blockFile.fd, 0L, SEEK_CUR), blockFile.name.c_str());
			lseek(blockFile.fd, -nbRead + 8, SEEK_CUR);
			uint64_t pos = lseek(blockFile.fd, 0L, SEEK_CUR);
			printf("[Decker][+] filepos = 0x%" PRIx64 " %s\n", pos, blockFile.name.c_str());
			auto nbRead = read(blockFile.fd, buf, sz);
				if (nbRead<(signed)sz) {
					//printf("\n[Decker][1] reading block #%d\n", nbBlocks);
					break;
				}
			}

		// edge case (file end, readed header contains only zeroes)
		// blk*.dat file can have less than 128 Mb length
		if ((*(uint32_t *)buf == 0) && (*(uint32_t *)(buf + 4) == 0)) {
			printf("[Decker][2] edge case #%d\n", nbBlocks);
			printf("[Decker][-] filepos = 0x%" PRIx64 " %s\n", lseek(blockFile.fd, 0L, SEEK_CUR), blockFile.name.c_str());
			lseek(blockFile.fd, -nbRead + 8, SEEK_CUR);
			break;
		}

	    #if (defined(KOMODO) || defined(DTT))
	    //buf_header_size = *(uint32_t *)&buf[4];
	    p = buf;
 	    
		//SKIP(uint32_t, magic, p); 
		LOAD(uint32_t, magic, p);
		if (unlikely(gExpectedMagic != magic)) {
			printf("\n");
			printf("[Decker][-] reading block #%d\n", nbBlocks);
			printf("[Decker][-] %d %s\n", blockFile.fd, blockFile.name.c_str());
			printf("[Decker][-] filepos = 0x%" PRIx64 "\n", lseek(blockFile.fd, 0L, SEEK_CUR));
			canonicalHexDump(buf, nbRead, "");
			exit(1);
		}


	    LOAD(uint32_t, buf_header_size, p); 
            
            buf_header = (uint8_t *)malloc(8+buf_header_size);
            lseek(blockFile.fd, -nbRead, SEEK_CUR);
			
            nbRead = read(blockFile.fd, buf_header, buf_header_size);
            if(nbRead<(signed)buf_header_size) {
				printf("\n[Decker][3] reading block #%d\n", nbBlocks);
				break;
            }
            //lseek(blockFile.fd, -nbRead, SEEK_CUR);

            //printf("[Decker] buf_header_size = %d\n",buf_header_size);
            //printf("[Decker] blkSolutionsize = %d\n",blkSolutionsize);

 	    //int dck_index;
            //for (dck_index = 0; dck_index < 8+gHeaderSize; dck_index++) printf("%02x",buf[dck_index]); printf("\n");
            //for (dck_index = 0; dck_index < 8+buf_header_size; dck_index++) printf("%02x",buf_header[dck_index]); printf("\n");
	    #endif

            startBlock((uint8_t*)0);

            uint8_t *hash = 0;
            Block *prevBlock = 0;
            size_t blockSize = 0;

            getBlockHeader(
                blockSize,
                prevBlock,
                hash,
                earlyMissCnt,
		#if (defined(KOMODO) || defined(DTT))
		buf_header
		#else
                buf
		#endif
            );
	    #if (defined(KOMODO) || defined(DTT))
	    free(buf_header);
		//printf("[Decker] blockSize = %lu\n", blockSize);

	    
	    
		// example of work with uint256_t
		//uint256_t missedHash;
		//fromHex(missedHash.v, (const uint8_t *)"046f2ea0280b5cb953474d7ac906bbc63b388875eeb0e74d276429685557b542");
		
		//if (0 == memcmp(missedHash.v, hash, sizeof(missedHash))) {
			
			//int dck_index;
			//printf("[Decker] blockSize = %lu\n", blockSize);
			//printf("prevblock hash: "); for (dck_index = 12 + 32 - 1; dck_index >= 12; dck_index--) printf("%02x", buf[dck_index]); printf("\n"); // prevblock hash
			//																																	  //printf("merklroot hash: "); for (dck_index = 12+32+32-1; dck_index >= 12+32; dck_index--) printf("%02x",buf[dck_index]); printf("\n"); // merkle root
			//if (unlikely(0 != hash)) {
			//	printf("    	  hash: "); for (dck_index = 31; dck_index >= 0; dck_index--) printf("%02x", hash[dck_index]); printf("\n");
			//}
			//printf("merklroot hash: "); for (dck_index = 12 + 32 + 32 - 1; dck_index >= 12 + 32; dck_index--) printf("%02x", buf[dck_index]); printf("\n"); // merkle root
			

		//}
            //printf("    	  hash: 027e3758c3a65b12aa1046462b486d0a63bfa1beae327897f56c5cfb7daaae71\n");
            //exit(0);
	    #endif

            if(unlikely(0==hash)) {
                printf("[Decker] End of scanning ...\n");
                break;
            }
            #if (defined(KOMODO) || defined(DTT))
			auto where = lseek(blockFile.fd, (blockSize + 8) - buf_header_size, SEEK_CUR);            
            #else
            auto where = lseek(blockFile.fd, (blockSize + 8) - sz, SEEK_CUR);
            #endif
            auto blockOffset = where - blockSize;
            if(where<0) {
				printf("\n[Decker][4] reading block #%d\n", nbBlocks);
                break;
            }
 	    //printf("[Decker] blockOffset = 0x%08X\n", blockOffset);
			
			/*
			hash = 96 e5 6a f1 b6 81 9f a1 72 17 07 ff 3a 8d 1b 96 ac d5 10 cf bd 00 59 b0 45 22 e2 6b 86 ab 3a 06
			hash = 063aab866be22245b05900bdcf10d5ac961b8d3aff071772a19f81b6f16ae596
					
			getblock "063aab866be22245b05900bdcf10d5ac961b8d3aff071772a19f81b6f16ae596"
					
			{
			"hash": "063aab866be22245b05900bdcf10d5ac961b8d3aff071772a19f81b6f16ae596",
			"confirmations": -1,
			"size": 3897,
			"height": 711314,
			"version": 4,
			"merkleroot": "bb89581693da34298f65ec2fcd2ec308f5f7dce281d64c4b8024eb4fd1ff919c",
			"tx": [
			"36a9482b0ff75598c727b44909cb5e88e567c0c7ac5c6e97594989394ca84980",
			"c58c198936c132b6209b6dd03802c4b8982781d224504fcbcbc80ddcc0ec08ee",
			"86fdedcf8c78403631ccd1a3bd51e269befc2eadc5b3c0c83c94e1b2e8dea59e",
			"1d29d0666c04de96f4dec2cf6e0689b1d9e81554777f0095845a457a01ad1d10",
			"bd11a72a82acf5e405f73c0f817f2f1f6d55413dfc443a3e28145b5e2efd7a41"
			],
			"time": 1518735074,
			"nonce": "000055d71a62318d9c20d1e1369c1fb233274e1730a44bb7dba3835c895b0000",
			"solution": "0110e90ed885de81e40111ace0e8f3147fdadd9c910d2f0684ffa420d1c82fc349c9f7ae64d54fae09c70fd5b661d5dbc97f376262fcac2bf17a17c3728a9830c95bf8dedf4ee1e80484a10c7c832215b612fe4b01a8915a3e824b4bfb9483b92c6ee0d95578ae1bd1240f3c74bf8af31ffcf9887b2de921bb0ea1dd70751c65bc5183dbf39f3845b5bf0867e142d59d3ee6161e68d3ab8388b9ffe3bed88404d9453624b658ae33016cb6b465c92a1ac4212a128ef69ed6f88398276a34eb1aa76b1d4aad1a298757bc76279dfec717c2b1070fcd5e0684fbf8c247626acb9e7c18acf44b864b24f3cc6200a7af31fea0a7e6eae835e747531a49590f6b5be95b126d52f37d323f9beee876468a97f7a9483b6bf2b0547e6bd5b885896d6faa2d80ddb66572375002caf4310815f15cf5d60263ef5e635a9418cb38597adb351852ef8cc34893d54d8e4e8bc258436e046b4677789109e3e35931067ecb766e23a0d221e156370468541e7e7fc97e55c37ecd5269814e11ca6025aa47d8eef1551fd730b2d8dc72494583bf51537e7522d7600ee31797d29fb8c363f09c7697c3baf87510f1e97dffe0d0fb6c5da6cf92bd41fdffa31c25c114da2fe4460b4ce9e94753907879b6897c8cfc356122db97fc2522bd41bd3635b78a5f35ba2237b47b0e26a382200d9d8347f286555eed4e6e961f8d9e71ce0640be06be8c374f11ceba4928eb1f52cda63ff7588d84a476e2add3e9882628e9a1e9668a490f9ddbf11d97e388a7534b6731f932cff8de0e05b01f13c3d5664e237df7a25f31311ee6cb9f4a7b0ab973db182a0a50da77686a779fdce214d16e29a67dd3735621761a1405b18b1489c926bd1223ace4a0792ae46e338d1eac1287dda40f719f06b501f44fd592debdbaa461232c5ce6d809a25e877922b409f5a3526b1e1fd66f01c3b89de5324aabd2c1a3e68425f87d3a80ae8d760b8e2c53cd8524206365977154d2d0f6c01a5f02000adf81fd8b888e5b526ac4c735eade32d57b7e8e1b3c0d0f0473778cfdd245a483d32b1a8e1f3a9aace50d0aae633f1b87895601d221951c6cab8148dd7cc7255e66fb4ea53905447b97b747682f3a6ef89ab99a204b2fbcf40afc58e7c35330e8cd734ec5c5fb6ced29b2839a53501aeb9398b469ae635661b603336a3f0397d88d0ff16347d7c9532e982a20a1aa3bb71676116b32a7ee4b22b67f85c74104e8358ae16d37a63e137ab245899c6860ee5b1544ed5747e7599fdeb021354711cbb62acff1e17c07741162282a15095ebe261c5a5a9ba14d1ea940e5662bcdbfb3a1e738fa168d2495bfefc9dfcbf7188938741851d49eb766db73032e820697fa98de9901ee73b4283d3d0e407cd9e7b43f6b83cfacb61fd9b5fd96b2f6730d5b2b3c5e96ab05dbc42893abc97da67f26a9b45c531fddaa7f0ad02e5625d110f8c879e5e6d39409e17e690ba5fffb861864ed82faf74981bfea335a81f0af96a3b19b69bf1a8aad54136588d37121a452924d97a22663ba38cb10bd86c2a897343783c9a13b4bd990ddf82e14590629a42c97fe79fa4de37ce45aa075d03a0a857ff4651110555aa145c629455345c1584a802d71e353e0af1b70c54f3dda1609827211d9a92e54f5f90a7e54420691459892e460d5797576c4d1e0b9ca73c6de56b108568fde61a6614fa000d567d82ee3b6c0e27b764d0edaffc4310c87e981afb44c55cfb5a528b109e2c51eb8a1818914fc958606740dbb2666c67ee71875b30946952d6564102d299ec34052bf67331f595bbe5d2fb8074c09ad30a3e856f7e141501ed3031098777a130eef011ca179c930da0487b9b8d1895d38f1e3a81dbb120798ddeaad5e85b228329a72eca0b20e8b88",
			"bits": "1d0b17d5",
			"difficulty": 22774991.62213874,
			"chainwork": "00000000000000000000000000000000000000000000000000007ad377a44a9e",
			"anchor": "2e95ed8702efb0eff9f87b4b3bfb04b9ffab13d973f4513dfcd2511dfd50f59f",
			"previousblockhash": "0000054201c43f5e48b74b62703ffd566da02911e1c5889fb79c6a108674062d"
			}

			// Orphaned (!)

			hash = 6b 66 7c ce 28 a9 da d9 f0 14 a8 e4 26 cb fb 84 ed 92 ab 1a 83 cb c0 31 b6 34 df 23 b5 ff 83 09
			hash = 0983ffb523df34b631c0cb831aab92ed84fbcb26e4a814f0d9daa928ce7c666b 

			[Decker][+] filepos = 0x4b2c06f C:\Users\ User\AppData\Roaming/Komodo/blocks/blk00013.dat
			[Decker] blockSize = 3897
			[Decker][+] filepos = 0x4b2cfb0 C:\Users\ User\AppData\Roaming/Komodo/blocks/blk00013.dat
			[Decker] blockSize = 1589

			warning: in block 0715978424b86a3aa2c976a63fea7ebc8d632c991194de7bca3c083998f0097b failed to locate parent block 046f2ea0280b5cb953474d7ac906bbc63b388875eeb0e74d276429685557b542
							  210928 -- 210927
			*/

            auto block = Block::alloc();
            block->init(hash, &blockFile, blockSize, prevBlock, blockOffset);
            gBlockMap[hash] = block;
            endBlock((uint8_t*)0);
            ++nbBlocks;
        }
        baseOffset += blockFile.size;

        auto now = Timer::usecs();
        auto elapsed = now - startTime;
        auto bytesPerSec = baseOffset / (elapsed*1e-6);
        auto bytesLeft = gChainSize - baseOffset;
        auto secsLeft = bytesLeft / bytesPerSec;
        fprintf(
            stderr,
            " %.2f%% (%.2f/%.2f Gigs) -- %6d blocks -- %.2f Megs/sec -- ETA %.0f secs -- ELAPSED %.0f secs            \r",
            (100.0*baseOffset)/gChainSize,
            baseOffset/(1000.0*oneMeg),
            gChainSize/(1000.0*oneMeg),
            (int)nbBlocks,
            bytesPerSec*1e-6,
            secsLeft,
            elapsed*1e-6
        );
        fflush(stderr);

        endBlockFile(0);
    }

    if(0==nbBlocks) {
        warning("found no blocks - giving up                                                       ");
        exit(1);
    }

    char msg[128];
    msg[0] = 0;
    if(0<earlyMissCnt) {
        sprintf(msg, ", %d early link misses", (int)earlyMissCnt);
    }

    auto elapsed = 1e-6*(Timer::usecs() - startTime);
    info(
        "pass 1 -- took %.0f secs, %6d blocks, %.2f Gigs, %.2f Megs/secs %s, mem=%.3f Gigs                                            ",
        elapsed,
        (int)nbBlocks,
        (gChainSize * 1e-9),
        (gChainSize * 1e-6) / elapsed,
        msg,
        getMem()
    );
}

static void buildNullBlock() {
    gBlockMap[gNullHash.v] = gNullBlock = Block::alloc();
    gNullBlock->init(gNullHash.v, 0, 0, 0, 0);
    gNullBlock->height = 0;
}

static void initHashtables() {

    info("initializing hash tables");

    gTXOMap.setEmptyKey(empty);
    gBlockMap.setEmptyKey(empty);

    auto kAvgBytesPerTX = 542.0;
    auto nbTxEstimate = (size_t)(1.1 * (gChainSize / kAvgBytesPerTX));
    if(gNeedUpstream) {
        gTXOMap.resize(nbTxEstimate);
    }

    auto kAvgBytesPerBlock = 140000;
    auto nbBlockEstimate = (size_t)(1.1 * (gChainSize / kAvgBytesPerBlock));
    gBlockMap.resize(nbBlockEstimate);

    info("estimated number of blocks = %.2fK", 1e-3*nbBlockEstimate);
    info("estimated number of transactions = %.2fM", 1e-6*nbTxEstimate);
    info("done initializing hash tables - mem = %.3f Gigs", getMem());
}

#if defined(__CYGWIN__)
    #include <sys/cygwin.h>
    #include <cygwin/version.h>
    static char *canonicalize_file_name(
        const char *fileName
    ) {
        auto r = (char*)cygwin_create_path(CCP_WIN_A_TO_POSIX, fileName);
        if(0==r) {
            errFatal("can't canonicalize path %s", fileName);
        }
        return r;
    }
#endif

#if defined(_WIN64)
    static char *canonicalize_file_name(
        const char *fileName
    ) {
        return strdup(fileName);
    }
#endif


static std::string getNormalizedDirName(
    const std::string &dirName
) {

    auto t = canonicalize_file_name(dirName.c_str());
    if(0==t) {
        errFatal(
            "problem accessing directory %s",
            dirName.c_str()
        );
    }

    auto r = std::string(t);
    free(t);

    auto sz = r.size();
    if(0<sz) {
        if('/'==r[sz-1]) {
            r = std::string(r, 0, sz-2);
        }
    }

    return r;
}

static std::string getBlockchainDir() {
    auto dir = getenv("BLOCKCHAIN_DIR");
    if(0==dir) {
        dir = getenv("HOME");
        if(0==dir) {
            errFatal("please  specify either env. variable HOME or BLOCKCHAIN_DIR");
        }
    }
    return getNormalizedDirName(
        dir              +
        std::string("/") +
        kCoinDirName
    );
}

static void findBlockFiles() {

    gChainSize = 0;

    auto blockChainDir = getBlockchainDir();
    auto blockDir = blockChainDir + std::string("/blocks");
    info("loading block chain from directory: %s", blockChainDir.c_str());

    struct stat statBuf;
    auto r = stat(blockDir.c_str(), &statBuf);
    auto oldStyle = (r<0 || !S_ISDIR(statBuf.st_mode));

    int blkDatId = (oldStyle ? 1 : 0);
    auto fmt = oldStyle ? "/blk%04d.dat" : "/blocks/blk%05d.dat";
    while(1) {

        char buf[64];
        sprintf(buf, fmt, blkDatId++);

        auto fileName = blockChainDir + std::string(buf) ;
#ifdef _MSC_VER
		auto fd = open(fileName.c_str(), O_RDONLY | O_BINARY);
#else
		auto fd = open(fileName.c_str(), O_RDONLY);
#endif 

        if(fd<0) {
            if(1<blkDatId) {
                break;
            }
            sysErrFatal(
                "failed to open block chain file %s",
                fileName.c_str()
            );
        }

        struct stat statBuf;
        auto r0 = fstat(fd, &statBuf);
        if(r0<0) {
            sysErrFatal(
                "failed to fstat block chain file %s",
                fileName.c_str()
            );
        }

        auto fileSize = statBuf.st_size;
	#if !defined(_WIN64)
	    auto r1 = posix_fadvise(fd, 0, fileSize, POSIX_FADV_NOREUSE);
	    if(r1<0) {
		warning(
		    "failed to posix_fadvise on block chain file %s",
		    fileName.c_str()
		);
	    }
	#endif

        BlockFile blockFile;
        blockFile.fd = fd;
        blockFile.size = fileSize;
        blockFile.name = fileName;
        blockFiles.push_back(blockFile);
        gChainSize += fileSize;
    }
    info("block chain size = %.3f Gigs", 1e-9*gChainSize);
}

static void cleanBlockFiles() {
    for(const auto &blockFile : blockFiles) {
        auto r = close(blockFile.fd);
        if(r<0) {
            sysErr(
                "failed to close block chain file %s",
                blockFile.name.c_str()
            );
        }
    }
}

int main(
    int   argc,
    char *argv[]
) {

    auto start = Timer::usecs();
    fprintf(stderr, "\n");
    info("mem at start = %.3f Gigs", getMem());

    initCallback(argc, argv);
    findBlockFiles();
    initHashtables();
    buildNullBlock();
    buildBlockHeaders();
    computeBlockHeights();
    wireLongestChain();
    parseLongestChain();
    cleanBlockFiles();

    auto elapsed = (Timer::usecs() - start)*1e-6;
    info("all done in %.2f seconds", elapsed);
    info("mem at end = %.3f Gigs\n", getMem());
    return 0;
}

