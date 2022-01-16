package upload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"go.uber.org/zap"
)

type Uploader struct {
	sugar         *zap.SugaredLogger
	accountName   string
	accountKey    string
	containerName string
}

func NewUploader(sugar *zap.SugaredLogger, accountName, accountKey, containerName string) *Uploader {
	return &Uploader{sugar, accountName, accountKey, containerName}
}

func (u *Uploader) Upload(filePath string) error {
	fileName := filepath.Base(filePath)

	credential, err := azblob.NewSharedKeyCredential(u.accountName, u.accountKey)
	if err != nil {
		return fmt.Errorf("failed to init credential; %w", err)
	}
	url := fmt.Sprintf("https://%s.blob.core.windows.net/", u.accountName)
	serviceClient, err := azblob.NewServiceClientWithSharedKey(url, credential, nil)
	if err != nil {
		return fmt.Errorf("failed to init client; %w", err)
	}
	container := serviceClient.NewContainerClient(u.containerName)

	file, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return fmt.Errorf("failed to open file; %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			u.sugar.Errorw("failed to close file", "error", err)
		}
	}()

	blockBlob := container.NewBlockBlobClient(fileName)
	option := azblob.HighLevelUploadToBlockBlobOption{}
	_, err = blockBlob.UploadFileToBlockBlob(context.Background(), file, option)
	if err != nil {
		return fmt.Errorf("failed to upload file; %w", err)
	}

	return nil
}
