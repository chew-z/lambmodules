package lambmodules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dmulholland/mp3lib"
	"github.com/gin-gonic/gin"
)

type partial struct {
	Iter          int
	Text          string
	SSML          string
	AccessURL     string
	AudioURL      string
	AudioFilePath string
}

type lesson struct {
	Iter      int
	Title     string
	Hash      string
	Text      string
	Urls      []string
	Partials  []partial
	AccessURL string
	AudioURL  string
	Timestamp int64
	Valid     int64
}

const (
	s3Region       = "eu-central-1"
	s3Bucket       = "suka.yoga.prana"
	linkExpiration = 15 // for debugging purposes it is short
	voice          = "Maja"
)

// TODO - better handle gin HTTP codes
func getLessonfromS3(c *gin.Context, n string) lesson {
	var k lesson
	var num int
	if n == "" {
		num = 0
	} else {
		num, _ = strconv.Atoi(n)
	}
	fn := fmt.Sprintf("lesson_%d", num)
	// Initialize AWS Session
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	if fileExists(sess, fn+".json") {
		log.Println("Found JSON ", fn+".json")
		js, err := getFileAsString(sess, fn+".json")
		if err == nil {
			json.Unmarshal([]byte(js), &k)
			log.Printf("%+v", k)
			// TODO - at minimum check k.AudioURL
			if k.AudioURL == "" {
				c.String(204, "No content")
				return k
			}
			err = refreshS3(sess, &k, fn)
			c.String(http.StatusOK, k.AudioURL)
			return k
		}
		c.String(500, "Error reading file from S3")
		return k
	}
	log.Println("Not found JSON ", fn+".json")
	c.String(403, "Not found")
	return k
}

// TODO - for lesson get JSON, check if each partial exists and merge into mp3 lesson, save
func createLesson(num int) lesson {
	var k lesson
	fn := fmt.Sprintf("lesson_%d", num)
	// Initialize AWS Session
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	if fileExists(sess, fn+".json") {
		// get JSON and return it if links are still valid
		log.Println("Found JSON ", fn+".json")
		js, err := getFileAsString(sess, fn+".json")
		if err == nil {
			var ks3 lesson
			log.Printf("Found %s.json saved to S3.", fn)
			json.Unmarshal([]byte(js), &ks3)
			log.Printf("%+v", ks3)
			// Now the question is if acccess links are still valid
			now := time.Now().Unix()
			if ks3.Valid > now {
				log.Printf("lesson's %s access links are still valid so you can have it.\n", ks3.Hash)
				return ks3
			}
			log.Println("Creating MP3 for ", fn+".json")
			k = mergeAudio(sess, ks3, fn)
			log.Println("Created new mp3 for lesson ", fn)
			err = refreshS3(sess, &k, fn)
			return k
		}
		log.Println(err.Error())
	}
	log.Println("ERROR! not found in S3 ", fn+".json")
	return k
}

//TODO - refactor - Merge all partials into leson (as mp3) and save to S3
func mergeAudio(s *session.Session, k lesson, fn string) lesson {
	partials := k.Partials
	log.Println(partials)
	merged := new(bytes.Buffer)
	var totalFrames uint32
	var totalBytes uint32
	var firstBitRate int
	var isVBR bool
	// Sort
	sort.Slice(partials, func(i, j int) bool {
		return k.Partials[i].Iter < k.Partials[j].Iter
	})
	for _, p := range partials {
		var audio []byte
		var err error
		// merge separate partial streams in one single file
		if p.AudioFilePath != "" && fileExists(s, p.AudioFilePath) {
			audio, err = getFile(s, p.AudioFilePath)
			if err != nil {
				log.Println(err.Error())
				break
			}
		} else {
			log.Println("Doing nothing")
			break
			// TODO - get external file from url
		}
		log.Println("Merging ", p.AudioFilePath)
		isFirstFrame := true
		b := bytes.NewReader(audio)
		for {
			// Read the next frame from the input file.
			//TODO - get mp3 from S3 or url as AudioStream (bytes)
			frame := mp3lib.NextFrame(b)
			if frame == nil {
				break
			}
			// TODO - is it necessary?
			// Skip the first frame if it's a VBR header.
			if isFirstFrame {
				isFirstFrame = false
				if mp3lib.IsXingHeader(frame) || mp3lib.IsVbriHeader(frame) {
					continue
				}
			}
			// If we detect more than one bitrate we'll need to add a VBR
			// header to the output file.
			if firstBitRate == 0 {
				firstBitRate = frame.BitRate
			} else if frame.BitRate != firstBitRate {
				isVBR = true
			}
			// Write the frame to the output stream
			_, err := merged.Write(frame.RawBytes)
			if err != nil {
				log.Println(err.Error())
			}
			totalFrames++
			totalBytes += uint32(len(frame.RawBytes))
		} // for (merging files)
		if isVBR {
			log.Println("All bells and whistles should be 22050Hz, 48k, mono - like Polly output. Cannot handle VBR at the moment")
			log.Println("Aborting!")
			return k
		}
	}
	// save merged stream to S3
	err := addAudiostreamToS3(s, ioutil.NopCloser(merged), fn+".mp3")
	if err != nil {
		log.Println(err.Error())
	}
	err = refreshS3(s, &k, fn)
	log.Println("lesson MP3 saved to S3. End of createLesson().")
	return k
}

// get mp3 file access link, save lesson as JSON to S3 with updated expiration
func refreshS3(s *session.Session, ks3 *lesson, fn string) error {

	link, err := getFileLink(s, fn+".mp3")
	if err != nil {
		log.Print(err.Error())
	}
	ks3.AudioURL = link
	log.Println(ks3.AudioURL)
	ks3.Timestamp = time.Now().Unix()
	ks3.Valid = linkExpiration*60 + ks3.Timestamp
	// Marshall lesson object to pretty JSON string
	js, err := json.MarshalIndent(ks3, "  ", "    ")
	if err != nil {
		log.Println(err.Error())
	}
	// save JSON to S3
	err = addTextToS3(s, string(js), fn+".json")
	if err != nil {
		log.Print(err.Error())
	}
	log.Println("lesson JSON saved to S3.")

	return err
}

// TODO - everything below goes to module
// Save Audiostream generated by Polly to S3
func addAudiostreamToS3(s *session.Session, pollyStream io.ReadCloser, fileName string) error {

	buffer := streamToByte(pollyStream)
	size := int64(len(buffer))

	// Config settings: this is where you choose the bucket, filename, content-type etc.
	// of the file you're uploading.
	_, err := s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(s3Bucket),
		Key:                  aws.String(fileName),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(buffer),
		ContentLength:        aws.Int64(size),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

// Save text (string) to S3
func addTextToS3(s *session.Session, comm string, fileName string) error {

	buffer := []byte(comm)
	size := int64(len(buffer))

	// Config settings: this is where you choose the bucket, filename, content-type etc.
	// of the file you're uploading.
	_, err := s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(s3Bucket),
		Key:                  aws.String(fileName),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(buffer),
		ContentLength:        aws.Int64(size),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

// Get public time-limited link to private object in S3 bucket
// if what you want is just the URL of a public access object you can build the URL yourself:
// https://<region>.amazonaws.com/<bucket-name>/<key>
func getFileLink(s *session.Session, fileName string) (string, error) {

	req, _ := s3.New(s).GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(fileName),
	})
	url, err := req.Presign(linkExpiration * time.Minute) // Set link expiration time

	return url, err
}

// return true if file exists
func fileExists(s *session.Session, fileName string) bool {

	_, err := s3.New(s).HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(fileName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return false
	}
	return true
}

// return file content from S3
func getFile(s *session.Session, fileName string) ([]byte, error) {
	buff := &aws.WriteAtBuffer{}
	s3dl := s3manager.NewDownloader(s)
	_, err := s3dl.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(fileName),
	})

	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

// return file content from S3 as string
func getFileAsString(s *session.Session, fileName string) (string, error) {
	buff := &aws.WriteAtBuffer{}
	s3dl := s3manager.NewDownloader(s)
	_, err := s3dl.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(fileName),
	})

	if err != nil {
		return "", err
	}

	return string(buff.Bytes()), nil
}

// convert AudioStreams to []byte
func streamToByte(stream io.Reader) []byte {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.Bytes()
}
