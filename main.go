package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	//choose the file you want.
	// pathFileS3 := "tmp/test-file-pdf.pdf"
	pathFileS3 := "tmp/test-file-image.jpg"

	pathNewFileS3 := pathFileS3
	pathFileLocal := "./content/" + filepath.Base(pathFileS3)
	if err := ReadFileAndUpload(pathFileLocal, pathNewFileS3); err != nil {
		log.Println(err)
		panic(err)
	}
	log.Println("read file and upload file success")

	fileInfo, err := GetFileInfo(pathFileS3)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	log.Println("get file info:", fileInfo)

	if err := GetFileToWrite(pathFileS3, "./temp/"); err != nil {
		log.Println(err)
		panic(err)
	}
	log.Println("get file and write file success")

	if err := RemoveFileFromS3(pathFileS3); err != nil {
		log.Println(err)
		panic(err)
	}
	log.Println("remove file on s3 success")
}

var region = "your-region-s3"                      //us-east-1
var s3Endpoint = "https://your_endpoint:your_port" //https://s3gb.com:8082
var s3AccessKey = "your_access_key"                //AT253AHWFMAAKD
var s3SecretKey = "your_secrect_key"               //dA2df/ZAK+LL78/12A+AKK
var s3BucketName = "your_bucket_name"              //my-bucket //sub folder in root s3

func ConnectS3() *session.Session {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile: "s3",
		Config: aws.Config{
			Region:                 aws.String(region),
			DisableParamValidation: aws.Bool(true),
			Endpoint:               aws.String(s3Endpoint),
			Credentials:            credentials.NewStaticCredentials(s3AccessKey, s3SecretKey, ""),
		},
		SharedConfigState: session.SharedConfigEnable,
	}))

	return sess
}

func GetFileInfo(pathFileS3 string) (*s3.GetObjectOutput, error) {
	var s3ObjectInfo *s3.GetObjectOutput

	sess := ConnectS3()
	svc := s3.New(sess)

	input := &s3.GetObjectInput{
		Bucket: aws.String(s3BucketName),
		Key:    aws.String(pathFileS3),
	}

	result, err := svc.GetObject(input)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
				return s3ObjectInfo, aerr
			default:
				log.Println(aerr.Error())
				return s3ObjectInfo, aerr
			}
		}
		log.Println(err.Error())
		return s3ObjectInfo, err
	}

	return result, nil

}

func GetFileToWrite(pathFileS3, pathFileToWrite string) error {

	s3ObjectInfo, err := GetFileInfo(pathFileS3)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	defer (s3ObjectInfo.Body).Close()

	//create directory
	if err = os.MkdirAll(filepath.Dir(pathFileToWrite), os.ModePerm); err != nil {
		log.Println(err)
		return err
	}

	//create file and random name file
	//another option -> os.Create
	tempFileCreate, err := os.CreateTemp(pathFileToWrite, "*"+filepath.Ext(pathFileS3))
	if err != nil {
		log.Println(err)
		return err
	}
	// tempFilePath := tempFileCreate.Name() // get temp file path

	_, err = io.Copy(tempFileCreate, s3ObjectInfo.Body)
	if err != nil {
		log.Println(err)
		return err
	}

	defer tempFileCreate.Close()

	return nil
}

func RemoveFileFromS3(pathFileS3 string) error {
	//check last value path file
	if len(pathFileS3) > 0 {
		if pathFileS3[len(pathFileS3)-1] == '/' {
			return nil
		}
	}

	//check file does not exist
	_, err := GetFileInfo(pathFileS3)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	sess := ConnectS3()
	svc := s3.New(sess)

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s3BucketName),
		Key:    aws.String(pathFileS3),
	})
	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}

func UploadFileToS3(filePathS3, contentType string, file io.Reader) (*s3manager.UploadOutput, error) {
	sess := ConnectS3()

	uploader := s3manager.NewUploader(sess)
	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(s3BucketName),
		Key:         aws.String(filePathS3),
		Body:        file,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return result, nil
}

func ReadFileAndUpload(pathFileLocal, filePathS3 string) error {

	file, err := os.Open(pathFileLocal)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer file.Close()

	contentType, err := GetFileContentType(file)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	_, err = UploadFileToS3(filePathS3, contentType, file)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	// //another option -> []byte
	// //use a lot of memory.
	// byteFile, err := ioutil.ReadFile(pathFileLocal)
	// if err != nil {
	// 	log.Println(err.Error())
	// 	return err
	// }

	// contentType := http.DetectContentType(byteFile)
	// _, err = UploadFileToS3(filePathS3, contentType, bytes.NewReader(byteFile))
	// if err != nil {
	// 	log.Println(err.Error())
	// 	return err
	// }

	return nil
}

func GetFileContentType(out *os.File) (string, error) {

	buffer := make([]byte, 512)

	_, err := out.Read(buffer)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}

	contentType := http.DetectContentType(buffer)

	return contentType, nil
}
